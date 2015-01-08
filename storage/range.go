// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Jiang-Ming Yang (jiangming.yang@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// init pre-registers RangeDescriptor, PrefixConfigMap types and Transaction.
func init() {
	gob.Register(StoreDescriptor{})
	gob.Register(PrefixConfigMap{})
	gob.Register(&proto.AcctConfig{})
	gob.Register(&proto.PermConfig{})
	gob.Register(&proto.ZoneConfig{})
	gob.Register(proto.RangeDescriptor{})
	gob.Register(proto.Transaction{})
}

var (
	// DefaultHeartbeatInterval is how often heartbeats are sent from the
	// transaction coordinator to a live transaction. These keep it from
	// being preempted by other transactions writing the same keys. If a
	// transaction fails to be heartbeat within 2x the heartbeat interval,
	// it may be aborted by conflicting txns.
	DefaultHeartbeatInterval = 5 * time.Second

	// ttlClusterIDGossip is time-to-live for cluster ID. The cluster ID
	// serves as the sentinel gossip key which informs a node whether or
	// not it's connected to the primary gossip network and not just a
	// partition. As such it must expire on a reasonable basis and be
	// continually re-gossipped. The replica which is the raft leader of
	// the first range gossips it.
	ttlClusterIDGossip = 30 * time.Second
)

// configDescriptor describes administrative configuration maps
// affecting ranges of the key-value map by key prefix.
type configDescriptor struct {
	keyPrefix proto.Key   // Range key prefix
	gossipKey string      // Gossip key
	configI   interface{} // Config struct interface
}

// configDescriptors is a slice containing the accounting, permissions
// and zone configuration descriptors.
var configDescriptors = []*configDescriptor{
	{engine.KeyConfigAccountingPrefix, gossip.KeyConfigAccounting, proto.AcctConfig{}},
	{engine.KeyConfigPermissionPrefix, gossip.KeyConfigPermission, proto.PermConfig{}},
	{engine.KeyConfigZonePrefix, gossip.KeyConfigZone, proto.ZoneConfig{}},
}

// tsCacheMethods specifies the set of methods which affect the
// timestamp cache.
var tsCacheMethods = map[string]struct{}{
	proto.Contains:              struct{}{},
	proto.Get:                   struct{}{},
	proto.Put:                   struct{}{},
	proto.ConditionalPut:        struct{}{},
	proto.Increment:             struct{}{},
	proto.Scan:                  struct{}{},
	proto.Delete:                struct{}{},
	proto.DeleteRange:           struct{}{},
	proto.ReapQueue:             struct{}{},
	proto.EnqueueUpdate:         struct{}{},
	proto.EnqueueMessage:        struct{}{},
	proto.InternalResolveIntent: struct{}{},
	proto.InternalMerge:         struct{}{},
}

// UsesTimestampCache returns true if the method affects or is
// affected by the timestamp cache.
func UsesTimestampCache(method string) bool {
	_, ok := tsCacheMethods[method]
	return ok
}

// A pendingCmd holds method, args, reply and a done channel for a command
// sent to Raft. Once committed to the Raft log, the command is
// executed and the result returned via the done channel.
type pendingCmd struct {
	Method string
	Args   proto.Request
	Reply  proto.Response
	done   chan error // Used to signal waiting RPC handler
}

// A RangeManager is an interface satisfied by Store through which ranges
// contained in the store can access the methods required for splitting.
type RangeManager interface {
	// Accessors for shared state.
	Allocator() *allocator
	Clock() *hlc.Clock
	ClusterID() string
	DB() *client.KV
	Engine() engine.Engine
	Gossip() *gossip.Gossip
	StoreID() int32

	// Range manipulation methods.
	AddRange(rng *Range) error
	CreateSnapshot() (string, error)
	MergeRange(subsumingRng *Range, updatedEndKey proto.Key, subsumedRaftID int64) error
	NewRangeDescriptor(start, end proto.Key, replicas []proto.Replica) (*proto.RangeDescriptor, error)
	ProposeRaftCommand(cmdIDKey, proto.InternalRaftCommand)
	RemoveRange(rng *Range) error
	SplitRange(origRng, newRng *Range) error
}

// A Range is a contiguous keyspace with writes managed via an
// instance of the Raft consensus algorithm. Many ranges may exist
// in a store and they are unlikely to be contiguous. Ranges are
// independent units and are responsible for maintaining their own
// integrity by replacing failed replicas, splitting and merging
// as appropriate.
type Range struct {
	Desc      *proto.RangeDescriptor
	rm        RangeManager  // Makes some store methods available
	splitting int32         // 1 if a split is underway; updated atomically
	closer    chan struct{} // Channel for closing the range

	sync.RWMutex                 // Protects the following fields (and Desc)
	cmdQ         *CommandQueue   // Enforce at most one command is running per key(s)
	tsCache      *TimestampCache // Most recent timestamps for keys / key ranges
	respCache    *ResponseCache  // Provides idempotence for retries
	pendingCmds  map[cmdIDKey]*pendingCmd
}

// NewRange initializes the range using the given metadata.
func NewRange(desc *proto.RangeDescriptor, rm RangeManager) *Range {
	r := &Range{
		Desc:        desc,
		rm:          rm,
		closer:      make(chan struct{}),
		cmdQ:        NewCommandQueue(),
		tsCache:     NewTimestampCache(rm.Clock()),
		respCache:   NewResponseCache(desc.RaftID, rm.Engine()),
		pendingCmds: map[cmdIDKey]*pendingCmd{},
	}
	return r
}

// Start begins gossiping loop in the event this is the first
// range in the map and gossips config information if the range
// contains any of the configuration maps.
func (r *Range) start() {
	r.maybeGossipClusterID()
	r.maybeGossipFirstRange()
	r.maybeGossipConfigs(configDescriptors...)
	// Only start gossiping if this range is the first range.
	if r.IsFirstRange() {
		go r.startGossip()
	}
}

// Stop ends the log processing loop.
func (r *Range) stop() {
	close(r.closer)
}

// Destroy cleans up all data associated with this range.
func (r *Range) Destroy() error {
	start := engine.MVCCEncodeKey(proto.Key(r.Desc.StartKey))
	end := engine.MVCCEncodeKey(proto.Key(r.Desc.EndKey))
	if _, err := engine.ClearRange(r.rm.Engine(), start, end); err != nil {
		return util.Errorf("unable to clear key/value data for range %d: %s", r.Desc.RaftID, err)
	}
	return r.SoftDestroy()
}

// SoftDestroy cleans up all metadata assocaited with this range, but leave the data
// intact.  The common use case for this function is when doing a merge between two adjacent
// ranges.
func (r *Range) SoftDestroy() error {
	start := engine.MVCCEncodeKey(engine.MakeKey(engine.KeyLocalTransactionPrefix, r.Desc.StartKey))
	end := engine.MVCCEncodeKey(engine.MakeKey(engine.KeyLocalTransactionPrefix, r.Desc.EndKey))
	if _, err := engine.ClearRange(r.rm.Engine(), start, end); err != nil {
		return util.Errorf("unable to clear txn records for range %d: %s", r.Desc.RaftID, err)
	}
	if err := r.respCache.ClearData(); err != nil {
		return util.Errorf("unable to clear response cache for range %d: %s", r.Desc.RaftID, err)
	}
	if err := engine.ClearRangeStats(r.rm.Engine(), r.Desc.RaftID); err != nil {
		return util.Errorf("unable to clear range stats for range %d: %s", r.Desc.RaftID, err)
	}
	start = engine.MVCCEncodeKey(engine.RangeDescriptorKey(r.Desc.StartKey))
	end = engine.MVCCEncodeKey(engine.RangeDescriptorKey(r.Desc.StartKey).Next())
	if _, err := engine.ClearRange(r.rm.Engine(), start, end); err != nil {
		return util.Errorf("unable to clear metadata for range %d: %s", r.Desc.RaftID, err)
	}
	return nil
}

// IsFirstRange returns true if this is the first range.
func (r *Range) IsFirstRange() bool {
	return bytes.Equal(r.Desc.StartKey, engine.KeyMin)
}

// IsLeader returns true if this range replica is the raft leader.
// TODO(spencer): this is always true for now.
func (r *Range) IsLeader() bool {
	return true
}

// GetReplica returns the replica for this range from the range descriptor.
func (r *Range) GetReplica() *proto.Replica {
	return r.Desc.FindReplica(r.rm.StoreID())
}

// ContainsKey returns whether this range contains the specified key.
func (r *Range) ContainsKey(key proto.Key) bool {
	// Read-lock the mutex to protect access to Desc, which might be changed
	// concurrently via range split.
	r.RLock()
	defer r.RUnlock()
	return r.Desc.ContainsKey(engine.KeyAddress(key))
}

// ContainsKeyRange returns whether this range contains the specified
// key range from start to end.
func (r *Range) ContainsKeyRange(start, end proto.Key) bool {
	r.RLock()
	defer r.RUnlock()
	return r.Desc.ContainsKeyRange(engine.KeyAddress(start), engine.KeyAddress(end))
}

// AddCmd adds a command for execution on this range. The command's
// affected keys are verified to be contained within the range and the
// range's leadership is confirmed. The command is then dispatched
// either along the read-only execution path or the read-write Raft
// command queue. If wait is false, read-write commands are added to
// Raft without waiting for their completion.
func (r *Range) AddCmd(method string, args proto.Request, reply proto.Response, wait bool) error {

	if !r.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		err := &proto.NotLeaderError{}
		reply.Header().SetGoError(err)
		return err
	}

	// Differentiate between read-only and read-write.
	if proto.IsAdmin(method) {
		return r.addAdminCmd(method, args, reply)
	} else if proto.IsReadOnly(method) {
		return r.addReadOnlyCmd(method, args, reply)
	}
	return r.addReadWriteCmd(method, args, reply, wait)
}

// beginCmd waits for any overlapping, already-executing commands via
// the command queue and adds itself to the queue to gate follow-on
// commands which overlap its key range. This method will block if
// there are any overlapping commands already in the queue. Returns
// the command queue insertion key, to be supplied to subsequent
// invocation of cmdQ.Remove().
func (r *Range) beginCmd(start, end proto.Key, readOnly bool) interface{} {
	r.Lock()
	var wg sync.WaitGroup
	r.cmdQ.GetWait(start, end, readOnly, &wg)
	cmdKey := r.cmdQ.Add(start, end, readOnly)
	r.Unlock()
	wg.Wait()
	return cmdKey
}

// addAdminCmd executes the command directly. There is no interaction
// with the command queue or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
func (r *Range) addAdminCmd(method string, args proto.Request, reply proto.Response) error {
	switch method {
	case proto.AdminSplit:
		r.AdminSplit(args.(*proto.AdminSplitRequest), reply.(*proto.AdminSplitResponse))
	case proto.AdminMerge:
		r.AdminMerge(args.(*proto.AdminMergeRequest), reply.(*proto.AdminMergeResponse))
	default:
		return util.Errorf("unrecognized admin command type: %s", method)
	}
	return reply.Header().GoError()
}

// addReadOnlyCmd updates the read timestamp cache and waits for any
// overlapping writes currently processing through Raft ahead of us to
// clear via the read queue.
func (r *Range) addReadOnlyCmd(method string, args proto.Request, reply proto.Response) error {
	header := args.Header()

	// Add the read to the command queue to gate subsequent
	// overlapping, commands until this command completes.
	cmdKey := r.beginCmd(header.Key, header.EndKey, true)

	// It's possible that arbitrary delays (e.g. major GC, VM
	// de-prioritization, etc.) could cause the execution of this read
	// command to occur AFTER the range replica has lost leadership.
	//
	// There is a chance that we waited on writes, and although they
	// were committed to the log, they weren't successfully applied to
	// this replica's state machine. We re-verify leadership before
	// reading to make sure that all pending writes are persisted.
	//
	// There are some elaborate cases where we might have lost
	// leadership and then regained it during the delay, but this is ok
	// because any writes during that period necessarily had higher
	// timestamps. This is because the read-timestamp-cache prevents it
	// for the active leader and leadership changes force the
	// read-timestamp-cache to reset its low water mark.
	if !r.IsLeader() {
		// TODO(spencer): when we happen to know the leader, fill it in here via replica.
		return &proto.NotLeaderError{}
	}
	err := r.executeCmd(method, args, reply)

	// Only update the timestamp cache if the command succeeded.
	r.Lock()
	if err == nil && UsesTimestampCache(method) {
		r.tsCache.Add(header.Key, header.EndKey, header.Timestamp, header.Txn.MD5(), true /* readOnly */)
	}
	r.cmdQ.Remove(cmdKey)
	r.Unlock()

	return err
}

// addReadWriteCmd first consults the response cache to determine whether
// this command has already been sent to the range. If a response is
// found, it's returned immediately and not submitted to raft. Next,
// the timestamp cache is checked to determine if any newer accesses to
// this command's affected keys have been made. If so, this command's
// timestamp is moved forward. Finally the keys affected by this
// command are added as pending writes to the read queue and the
// command is submitted to Raft. Upon completion, the write is removed
// from the read queue and the reply is added to the response cache.
// If wait is true, will block until the command is complete.
func (r *Range) addReadWriteCmd(method string, args proto.Request, reply proto.Response, wait bool) error {
	// Check the response cache in case this is a replay. This call
	// may block if the same command is already underway.
	header := args.Header()
	txnMD5 := header.Txn.MD5()
	if ok, err := r.respCache.GetResponse(header.CmdID, reply); ok || err != nil {
		if ok { // this is a replay! extract error for return
			return reply.Header().GoError()
		}
		// In this case there was an error reading from the response
		// cache. Instead of failing the request just because we can't
		// decode the reply in the response cache, we proceed as though
		// idempotence has expired.
		log.Errorf("unable to read result for %+v from the response cache: %s", args, err)
	}

	// Add the write to the command queue to gate subsequent overlapping
	// commands until this command completes. Note that this must be
	// done before getting the max timestamp for the key(s), as
	// timestamp cache is only updated after preceding commands have
	// been run to successful completion.
	cmdKey := r.beginCmd(header.Key, header.EndKey, false)

	// Two important invariants of Cockroach: 1) encountering a more
	// recently written value means transaction restart. 2) values must
	// be written with a greater timestamp than the most recent read to
	// the same key. Check the timestamp cache for reads/writes which
	// are at least as recent as the timestamp of this write. For
	// writes, send WriteTooOldError; for reads, update the write's
	// timestamp. When the write returns, the updated timestamp will
	// inform the final commit timestamp.
	if UsesTimestampCache(method) {
		r.Lock()
		rTS, wTS := r.tsCache.GetMax(header.Key, header.EndKey, txnMD5)
		r.Unlock()

		// If there's a newer write timestamp and we're in a txn, set a
		// write too old error in reply. We still go ahead and try the
		// write; afterall, the cause of the higher timestamp may be an
		// intent we can push.
		if !wTS.Less(header.Timestamp) && header.Txn != nil {
			err := &proto.WriteTooOldError{Timestamp: header.Timestamp, ExistingTimestamp: wTS}
			reply.Header().SetGoError(err)
		} else if !wTS.Less(header.Timestamp) || !rTS.Less(header.Timestamp) {
			// Otherwise, make sure we advance the request's timestamp.
			ts := wTS
			if ts.Less(rTS) {
				ts = rTS
			}
			if log.V(1) {
				log.Infof("Overriding existing timestamp %s with %s", header.Timestamp, ts)
			}
			ts.Logical++ // increment logical component by one to differentiate.
			// Update the request timestamp.
			header.Timestamp = ts
		}
	}

	// Create command and enqueue for Raft.
	pendingCmd := &pendingCmd{
		Method: method,
		Args:   args,
		Reply:  reply,
		done:   make(chan error, 1),
	}
	raftCmd := proto.InternalRaftCommand{
		RaftID: r.Desc.RaftID,
	}
	var cmdID proto.ClientCmdID
	if !args.Header().CmdID.IsEmpty() {
		cmdID = args.Header().CmdID
	} else {
		cmdID = proto.ClientCmdID{
			WallTime: r.rm.Clock().PhysicalNow(),
			Random:   rand.Int63(),
		}
	}
	idKey := makeCmdIDKey(cmdID)
	r.Lock()
	r.pendingCmds[idKey] = pendingCmd
	r.Unlock()
	// TODO(bdarnell): In certain raft failover scenarios, proposed
	// commands may be abandoned. We need to re-propose the command
	// if too much time passes with no response on the done channel.
	r.rm.ProposeRaftCommand(idKey, raftCmd)

	// Create a completion func for mandatory cleanups which we either
	// run synchronously if we're waiting or in a goroutine otherwise.
	completionFunc := func() error {
		err := <-pendingCmd.done

		// As for reads, update timestamp cache with the timestamp
		// of this write on success. This ensures a strictly higher
		// timestamp for successive writes to the same key or key range.
		r.Lock()
		if err == nil && UsesTimestampCache(method) {
			r.tsCache.Add(header.Key, header.EndKey, header.Timestamp, txnMD5, false /* !readOnly */)
		}
		r.cmdQ.Remove(cmdKey)
		r.Unlock()

		// If the original client didn't wait (e.g. resolve write intent),
		// log execution errors so they're surfaced somewhere.
		if !wait && err != nil {
			log.Warningf("non-synchronous execution of %s with %+v failed: %s",
				pendingCmd.Method, pendingCmd.Args, err)
		}
		return err
	}

	if wait {
		return completionFunc()
	}
	go completionFunc()
	return nil
}

func (r *Range) processRaftCommand(idKey cmdIDKey, raftCmd proto.InternalRaftCommand) {
	r.Lock()
	cmd := r.pendingCmds[idKey]
	delete(r.pendingCmds, idKey)
	r.Unlock()
	var method string
	var args proto.Request
	var reply proto.Response
	var err error
	if cmd != nil {
		// We initiated this command, so use the caller-supplied reply.
		method = cmd.Method
		args = cmd.Args
		reply = cmd.Reply
	} else {
		// This command originated elsewhere so we must create a new
		// reply buffer and reconstruct the method name.
		args = raftCmd.Cmd.GetValue().(proto.Request)
		method, err = proto.MethodForRequest(args)
		if err != nil {
			log.Fatal(err)
		}
		_, reply, err = proto.CreateArgsAndReply(method)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = r.executeCmd(method, args, reply)
	if cmd != nil {
		cmd.done <- err
	} else if err != nil {
		log.Errorf("error executing raft command: %s", err)
	}
}

// startGossip periodically gossips the cluster ID if it's the
// first range and the raft leader.
func (r *Range) startGossip() {
	ticker := time.NewTicker(ttlClusterIDGossip / 2)
	for {
		select {
		case <-ticker.C:
			r.maybeGossipClusterID()
			r.maybeGossipFirstRange()
		case <-r.closer:
			return
		}
	}
}

// maybeGossipClusterID gossips the cluster ID if this range is
// the start of the key space and the raft leader.
func (r *Range) maybeGossipClusterID() {
	if r.rm.Gossip() != nil && r.IsFirstRange() && r.IsLeader() {
		if err := r.rm.Gossip().AddInfo(gossip.KeyClusterID, r.rm.ClusterID(), ttlClusterIDGossip); err != nil {
			log.Errorf("failed to gossip cluster ID %s: %s", r.rm.ClusterID(), err)
		}
	}
}

// maybeGossipFirstRange gossips the range locations if this range is
// the start of the key space and the raft leader.
func (r *Range) maybeGossipFirstRange() {
	if r.rm.Gossip() != nil && r.IsFirstRange() && r.IsLeader() {
		if err := r.rm.Gossip().AddInfo(gossip.KeyFirstRangeDescriptor, *r.Desc, 0*time.Second); err != nil {
			log.Errorf("failed to gossip first range metadata: %s", err)
		}
	}
}

// maybeGossipConfigs gossips configuration maps if their data falls
// within the range, this replica is the raft leader, and their
// contents are marked dirty. Configuration maps include accounting,
// permissions, and zones.
func (r *Range) maybeGossipConfigs(dirtyConfigs ...*configDescriptor) {
	if r.rm.Gossip() != nil && r.IsLeader() {
		for _, cd := range dirtyConfigs {
			if r.ContainsKey(cd.keyPrefix) {
				// Check for a bad range split. This should never happen as ranges
				// cannot be split mid-config.
				if !r.ContainsKey(cd.keyPrefix.PrefixEnd()) {
					log.Fatalf("range splits configuration values for %q", cd.keyPrefix)
				}
				configMap, err := r.loadConfigMap(cd.keyPrefix, cd.configI)
				if err != nil {
					log.Errorf("failed loading %s config map: %s", cd.gossipKey, err)
					continue
				} else {
					if err := r.rm.Gossip().AddInfo(cd.gossipKey, configMap, 0*time.Second); err != nil {
						log.Errorf("failed to gossip %s configMap: %s", cd.gossipKey, err)
						continue
					}
				}
			}
		}
	}
}

// loadConfigMap scans the config entries under keyPrefix and
// instantiates/returns a config map. Prefix configuration maps
// include accounting, permissions, and zones.
func (r *Range) loadConfigMap(keyPrefix proto.Key, configI interface{}) (PrefixConfigMap, error) {
	kvs, err := engine.MVCCScan(r.rm.Engine(), keyPrefix, keyPrefix.PrefixEnd(), 0, proto.MaxTimestamp, nil)
	if err != nil {
		return nil, err
	}
	var configs []*PrefixConfig
	for _, kv := range kvs {
		// Instantiate an instance of the config type by unmarshalling
		// proto encoded config from the Value into a new instance of configI.
		config := reflect.New(reflect.TypeOf(configI)).Interface().(gogoproto.Message)
		if err := gogoproto.Unmarshal(kv.Value.Bytes, config); err != nil {
			return nil, util.Errorf("unable to unmarshal config key %s: %s", string(kv.Key), err)
		}
		configs = append(configs, &PrefixConfig{Prefix: bytes.TrimPrefix(kv.Key, keyPrefix), Config: config})
	}
	return NewPrefixConfigMap(configs)
}

// maybeUpdateGossipConfigs is used to update gossip configs.
func (r *Range) maybeUpdateGossipConfigs(key proto.Key) {
	// Check whether this put has modified a configuration map.
	for _, cd := range configDescriptors {
		if bytes.HasPrefix(key, cd.keyPrefix) {
			r.maybeGossipConfigs(cd)
			break
		}
	}
}

// ShouldSplit returns whether the current size of the range exceeds
// the max size specified in the zone config.
func (r *Range) ShouldSplit() bool {
	// If not the leader or gossip is not enabled, ignore.
	if !r.IsLeader() || r.rm.Gossip() == nil {
		return false
	}

	// Fetch the zone config for the zone containing this range's start key.
	zoneMap, err := r.rm.Gossip().GetInfo(gossip.KeyConfigZone)
	if err != nil || zoneMap == nil {
		log.Errorf("unable to fetch zone config from gossip: %s", err)
		return false
	}
	prefixConfig := zoneMap.(PrefixConfigMap).MatchByPrefix(r.Desc.StartKey)
	zone := prefixConfig.Config.(*proto.ZoneConfig)

	// Fetch the current size of this range in total bytes.
	rangeSize, err := engine.GetRangeSize(r.rm.Engine(), r.Desc.RaftID)
	if err != nil {
		log.Errorf("unable to compute size from stats for range %d: %s", r.Desc.RaftID, err)
		return false
	}

	return rangeSize > zone.RangeMaxBytes
}

// maybeSplit initiates an asynchronous split via AdminSplit request
// if ShouldSplit is true. This operation is invoked after each
// successful execution of a read/write command.
func (r *Range) maybeSplit() {
	// If we're already splitting, ignore.
	if atomic.LoadInt32(&r.splitting) == int32(1) {
		return
	}
	// If this zone's total bytes are in excess, split the range. We omit
	// the split key in order to have AdminSplit determine it via scan
	// of range data.
	if r.ShouldSplit() {
		// Admin commands run synchronously, so run this in a goroutine.
		go r.AddCmd(proto.AdminSplit, &proto.AdminSplitRequest{
			RequestHeader: proto.RequestHeader{Key: r.Desc.StartKey},
		}, &proto.AdminSplitResponse{}, false)
	}
}

// executeCmd switches over the method and multiplexes to execute the
// appropriate storage API command.
//
// TODO(Spencer): Differentiate between errors caused by the normal culprits --
// bad inputs from clients, stale information, etc. and errors which might
// cause the range replicas to diverge -- running out of disk space, underlying
// rocksdb corruption, etc. Do a careful code audit to make sure we identify
// errors which should be classified as a ReplicaCorruptionError--when those
// bubble up to the point where we've just tried to execute a Raft command, the
// Raft replica would need to stall itself.
func (r *Range) executeCmd(method string, args proto.Request, reply proto.Response) error {
	// Verify key is contained within range here to catch any range split
	// or merge activity.
	header := args.Header()
	if !r.ContainsKeyRange(header.Key, header.EndKey) {
		err := proto.NewRangeKeyMismatchError(header.Key, header.EndKey, r.Desc)
		reply.Header().SetGoError(err)
		return err
	}

	// Create a new batch for the command to ensure all or nothing semantics.
	batch := r.rm.Engine().NewBatch()
	// Create an engine.MVCCStats instance.
	ms := &engine.MVCCStats{}

	switch method {
	case proto.Contains:
		r.Contains(batch, args.(*proto.ContainsRequest), reply.(*proto.ContainsResponse))
	case proto.Get:
		r.Get(batch, args.(*proto.GetRequest), reply.(*proto.GetResponse))
	case proto.Put:
		r.Put(batch, ms, args.(*proto.PutRequest), reply.(*proto.PutResponse))
	case proto.ConditionalPut:
		r.ConditionalPut(batch, ms, args.(*proto.ConditionalPutRequest), reply.(*proto.ConditionalPutResponse))
	case proto.Increment:
		r.Increment(batch, ms, args.(*proto.IncrementRequest), reply.(*proto.IncrementResponse))
	case proto.Delete:
		r.Delete(batch, ms, args.(*proto.DeleteRequest), reply.(*proto.DeleteResponse))
	case proto.DeleteRange:
		r.DeleteRange(batch, ms, args.(*proto.DeleteRangeRequest), reply.(*proto.DeleteRangeResponse))
	case proto.Scan:
		r.Scan(batch, args.(*proto.ScanRequest), reply.(*proto.ScanResponse))
	case proto.EndTransaction:
		r.EndTransaction(batch, args.(*proto.EndTransactionRequest), reply.(*proto.EndTransactionResponse))
	case proto.ReapQueue:
		r.ReapQueue(batch, args.(*proto.ReapQueueRequest), reply.(*proto.ReapQueueResponse))
	case proto.EnqueueUpdate:
		r.EnqueueUpdate(batch, args.(*proto.EnqueueUpdateRequest), reply.(*proto.EnqueueUpdateResponse))
	case proto.EnqueueMessage:
		r.EnqueueMessage(batch, args.(*proto.EnqueueMessageRequest), reply.(*proto.EnqueueMessageResponse))
	case proto.InternalRangeLookup:
		r.InternalRangeLookup(batch, args.(*proto.InternalRangeLookupRequest), reply.(*proto.InternalRangeLookupResponse))
	case proto.InternalHeartbeatTxn:
		r.InternalHeartbeatTxn(batch, args.(*proto.InternalHeartbeatTxnRequest), reply.(*proto.InternalHeartbeatTxnResponse))
	case proto.InternalPushTxn:
		r.InternalPushTxn(batch, args.(*proto.InternalPushTxnRequest), reply.(*proto.InternalPushTxnResponse))
	case proto.InternalResolveIntent:
		r.InternalResolveIntent(batch, ms, args.(*proto.InternalResolveIntentRequest), reply.(*proto.InternalResolveIntentResponse))
	case proto.InternalSnapshotCopy:
		r.InternalSnapshotCopy(r.rm.Engine(), args.(*proto.InternalSnapshotCopyRequest), reply.(*proto.InternalSnapshotCopyResponse))
	case proto.InternalMerge:
		r.InternalMerge(batch, ms, args.(*proto.InternalMergeRequest), reply.(*proto.InternalMergeResponse))
	default:
		return util.Errorf("unrecognized command %q", method)
	}

	// On success, flush the MVCC stats to the batch and commit.
	if err := reply.Header().GoError(); err == nil {
		if proto.IsReadWrite(method) {
			ms.MergeStats(batch, r.Desc.RaftID, r.rm.StoreID())
			if err := batch.Commit(); err != nil {
				reply.Header().SetGoError(err)
			} else {
				// If the commit succeeded, potentially initiate a split of this range.
				r.maybeSplit()
			}
		}
	} else if err, ok := reply.Header().GoError().(*proto.ReadWithinUncertaintyIntervalError); ok {
		// A ReadUncertaintyIntervalError contains the timestamp of the value
		// that provoked the conflict. However, we forward the timestamp to the
		// node's time here. The reason is that the caller (which is always
		// transactional when this error occurs) in our implementation wants to
		// use this information to extract a timestamp after which reads from
		// the nodes are causally consistent with the transaction. This allows
		// the node to be classified as without further uncertain reads for the
		// remainder of the transaction.
		// See the comment on proto.Transaction.CertainNodes.
		err.ExistingTimestamp.Forward(r.rm.Clock().Now())
	}

	// Maybe update gossip configs on a put if there was no error.
	if (method == proto.Put || method == proto.ConditionalPut) &&
		header.Key.Less(engine.KeySystemMax) && reply.Header().Error == nil {
		r.maybeUpdateGossipConfigs(args.Header().Key)
	}

	// Propagate the request timestamp (which may have changed).
	reply.Header().Timestamp = args.Header().Timestamp

	log.V(1).Infof("executed %s command %+v: %+v", method, args, reply)

	// Add this command's result to the response cache if this is a
	// read/write method. This must be done as part of the execution of
	// raft commands so that every replica maintains the same responses
	// to continue request idempotence when leadership changes.
	if proto.IsReadWrite(method) {
		if putErr := r.respCache.PutResponse(args.Header().CmdID, reply); putErr != nil {
			log.Errorf("unable to write result of %+v: %+v to the response cache: %s",
				args, reply, putErr)
		}
	}

	// Return the error (if any) set in the reply.
	return reply.Header().GoError()
}

// Contains verifies the existence of a key in the key value store.
func (r *Range) Contains(batch engine.Engine, args *proto.ContainsRequest, reply *proto.ContainsResponse) {
	val, err := engine.MVCCGet(batch, args.Key, args.Timestamp, args.Txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if val != nil {
		reply.Exists = true
	}
}

// Get returns the value for a specified key.
func (r *Range) Get(batch engine.Engine, args *proto.GetRequest, reply *proto.GetResponse) {
	val, err := engine.MVCCGet(batch, args.Key, args.Timestamp, args.Txn)
	reply.Value = val
	reply.SetGoError(err)
}

// Put sets the value for a specified key.
func (r *Range) Put(batch engine.Engine, ms *engine.MVCCStats, args *proto.PutRequest, reply *proto.PutResponse) {
	err := engine.MVCCPut(batch, ms, args.Key, args.Timestamp, args.Value, args.Txn)
	reply.SetGoError(err)
}

// ConditionalPut sets the value for a specified key only if
// the expected value matches. If not, the return value contains
// the actual value.
func (r *Range) ConditionalPut(batch engine.Engine, ms *engine.MVCCStats, args *proto.ConditionalPutRequest, reply *proto.ConditionalPutResponse) {
	val, err := engine.MVCCConditionalPut(batch, ms, args.Key, args.Timestamp, args.Value, args.ExpValue, args.Txn)
	reply.ActualValue = val
	reply.SetGoError(err)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func (r *Range) Increment(batch engine.Engine, ms *engine.MVCCStats, args *proto.IncrementRequest, reply *proto.IncrementResponse) {
	val, err := engine.MVCCIncrement(batch, ms, args.Key, args.Timestamp, args.Txn, args.Increment)
	reply.NewValue = val
	reply.SetGoError(err)
}

// Delete deletes the key and value specified by key.
func (r *Range) Delete(batch engine.Engine, ms *engine.MVCCStats, args *proto.DeleteRequest, reply *proto.DeleteResponse) {
	reply.SetGoError(engine.MVCCDelete(batch, ms, args.Key, args.Timestamp, args.Txn))
}

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func (r *Range) DeleteRange(batch engine.Engine, ms *engine.MVCCStats, args *proto.DeleteRangeRequest, reply *proto.DeleteRangeResponse) {
	num, err := engine.MVCCDeleteRange(batch, ms, args.Key, args.EndKey, args.MaxEntriesToDelete, args.Timestamp, args.Txn)
	reply.NumDeleted = num
	reply.SetGoError(err)
}

// Scan scans the key range specified by start key through end key up
// to some maximum number of results. The last key of the iteration is
// returned with the reply.
func (r *Range) Scan(batch engine.Engine, args *proto.ScanRequest, reply *proto.ScanResponse) {
	kvs, err := engine.MVCCScan(batch, args.Key, args.EndKey, args.MaxResults, args.Timestamp, args.Txn)
	reply.Rows = kvs
	reply.SetGoError(err)
}

// EndTransaction either commits or aborts (rolls back) an extant
// transaction according to the args.Commit parameter.
func (r *Range) EndTransaction(batch engine.Engine, args *proto.EndTransactionRequest, reply *proto.EndTransactionResponse) {
	if args.Txn == nil {
		reply.SetGoError(util.Errorf("no transaction specified to EndTransaction"))
		return
	}
	key := engine.MakeKey(engine.KeyLocalTransactionPrefix, args.Txn.Key, args.Txn.ID)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, nil, existTxn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	// If the transaction record already exists, verify that we can either
	// commit it or abort it (according to args.Commit), and also that the
	// Timestamp and Epoch have not suffered regression.
	if ok {
		// Use the persisted transaction record as final transaction.
		reply.Txn = gogoproto.Clone(existTxn).(*proto.Transaction)

		if existTxn.Status == proto.COMMITTED {
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, "already committed"))
			return
		} else if existTxn.Status == proto.ABORTED {
			reply.SetGoError(proto.NewTransactionAbortedError(existTxn))
			return
		} else if args.Txn.Epoch < existTxn.Epoch {
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("epoch regression: %d", args.Txn.Epoch)))
			return
		} else if existTxn.Timestamp.Less(args.Txn.Timestamp) {
			// The transaction record can only ever be pushed forward, so it's an
			// error if somehow the transaction record has an earlier timestamp
			// than the transaction timestamp.
			reply.SetGoError(proto.NewTransactionStatusError(existTxn, fmt.Sprintf("timestamp regression: %s", args.Txn.Timestamp)))
			return
		}
		// Take max of requested epoch and existing epoch. The requester
		// may have incremented the epoch on retries.
		if reply.Txn.Epoch < args.Txn.Epoch {
			reply.Txn.Epoch = args.Txn.Epoch
		}
		// Take max of requested priority and existing priority. This isn't
		// terribly useful, but we do it for completeness.
		if reply.Txn.Priority < args.Txn.Priority {
			reply.Txn.Priority = args.Txn.Priority
		}
	} else {
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.Txn = gogoproto.Clone(args.Txn).(*proto.Transaction)
	}

	// Take max of requested timestamp and possibly "pushed" txn
	// record timestamp as the final commit timestamp.
	if reply.Txn.Timestamp.Less(args.Timestamp) {
		reply.Txn.Timestamp = args.Timestamp
	}

	// Set transaction status to COMMITTED or ABORTED as per the
	// args.Commit parameter.
	if args.Commit {
		// If the isolation level is SERIALIZABLE, return a transaction
		// retry error if the commit timestamp isn't equal to the txn
		// timestamp.
		if args.Txn.Isolation == proto.SERIALIZABLE && !reply.Txn.Timestamp.Equal(args.Txn.OrigTimestamp) {
			reply.SetGoError(proto.NewTransactionRetryError(reply.Txn))
			return
		}
		reply.Txn.Status = proto.COMMITTED
	} else {
		reply.Txn.Status = proto.ABORTED
	}

	// Persist the transaction record with updated status (& possibly timestamp).
	if err := engine.MVCCPutProto(batch, nil, key, proto.ZeroTimestamp, nil, reply.Txn); err != nil {
		reply.SetGoError(err)
		return
	}

	// Run triggers if successfully committed. Any failures running
	// triggers will set an error and prevent the batch from committing.
	if reply.Txn.Status == proto.COMMITTED {
		if args.SplitTrigger != nil {
			reply.SetGoError(r.splitTrigger(batch, args.SplitTrigger))
		} else if args.MergeTrigger != nil {
			reply.SetGoError(r.mergeTrigger(batch, args.MergeTrigger))
		}
	}
}

// ReapQueue destructively queries messages from a delivery inbox
// queue. This method must be called from within a transaction.
func (r *Range) ReapQueue(batch engine.Engine, args *proto.ReapQueueRequest, reply *proto.ReapQueueResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// EnqueueUpdate sidelines an update for asynchronous execution.
// AccumulateTS updates are sent this way. Eventually-consistent indexes
// are also built using update queues. Crucially, the enqueue happens
// as part of the caller's transaction, so is guaranteed to be
// executed if the transaction succeeded.
func (r *Range) EnqueueUpdate(batch engine.Engine, args *proto.EnqueueUpdateRequest, reply *proto.EnqueueUpdateResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// EnqueueMessage enqueues a message (Value) for delivery to a
// recipient inbox.
func (r *Range) EnqueueMessage(batch engine.Engine, args *proto.EnqueueMessageRequest, reply *proto.EnqueueMessageResponse) {
	reply.SetGoError(util.Error("unimplemented"))
}

// InternalRangeLookup is used to look up RangeDescriptors - a RangeDescriptor
// is a metadata structure which describes the key range and replica locations
// of a distinct range in the cluster.
//
// RangeDescriptors are stored as values in the cockroach cluster's key-value
// store. However, they are always stored using special "Range Metadata keys",
// which are "ordinary" keys with a special prefix appended. The Range Metadata
// Key for an ordinary key can be generated with the `engine.RangeMetaKey(key)`
// function. The RangeDescriptor for the range which contains a given key can be
// retrieved by generating its Range Metadata Key and dispatching it to
// InternalRangeLookup.
//
// Note that the Range Metadata Key sent to InternalRangeLookup is NOT the key
// at which the desired RangeDescriptor is stored. Instead, this method returns
// the RangeDescriptor stored at the _lowest_ existing key which is _greater_
// than the given key. The returned RangeDescriptor will thus contain the
// ordinary key which was originally used to generate the Range Metadata Key
// sent to InternalRangeLookup.
//
// This method has an important optimization: instead of just returning the
// request RangeDescriptor, it also returns a slice of additional range
// descriptors immediately consecutive to the desired RangeDescriptor. This is
// intended to serve as a sort of caching pre-fetch, so that the requesting
// nodes can aggressively cache RangeDescriptors which are likely to be desired
// by their current workload.
func (r *Range) InternalRangeLookup(batch engine.Engine, args *proto.InternalRangeLookupRequest, reply *proto.InternalRangeLookupResponse) {
	if err := engine.ValidateRangeMetaKey(args.Key); err != nil {
		reply.SetGoError(err)
		return
	}

	rangeCount := int64(args.MaxRanges)
	if rangeCount < 1 {
		reply.SetGoError(util.Errorf(
			"Range lookup specified invalid maximum range count %d: must be > 0", rangeCount))
		return
	}

	// We want to search for the metadata key just greater than args.Key. Scan
	// for both the requested key and the keys immediately afterwards, up to
	// MaxRanges.
	metaPrefix := proto.Key(args.Key[:len(engine.KeyMeta1Prefix)])
	nextKey := proto.Key(args.Key).Next()
	kvs, err := engine.MVCCScan(batch, nextKey, metaPrefix.PrefixEnd(), rangeCount, args.Timestamp, args.Txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}

	// The initial key must have the same metadata level prefix as we queried.
	if len(kvs) == 0 {
		// At this point the range has been verified to contain the requested
		// key, but no matching results were returned from the scan. This could
		// indicate a very bad system error, but for now we will just treat it
		// as a retryable Key Mismatch error.
		err := proto.NewRangeKeyMismatchError(args.Key, args.EndKey, r.Desc)
		reply.SetGoError(err)
		log.Errorf("InternalRangeLookup dispatched to correct range, but no matching RangeDescriptor was found. %s", err)
		return
	}

	// Decode all scanned range descriptors, stopping if a range is encountered
	// which does not have the same metadata prefix as the queried key.
	rds := make([]proto.RangeDescriptor, len(kvs))
	for i := range kvs {
		if err = gogoproto.Unmarshal(kvs[i].Value.Bytes, &rds[i]); err != nil {
			reply.SetGoError(err)
			return
		}
	}

	reply.Ranges = rds
	return
}

// InternalHeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func (r *Range) InternalHeartbeatTxn(batch engine.Engine, args *proto.InternalHeartbeatTxnRequest, reply *proto.InternalHeartbeatTxnResponse) {
	key := engine.MakeKey(engine.KeyLocalTransactionPrefix, args.Txn.Key, args.Txn.ID)

	var txn proto.Transaction
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, nil, &txn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	// If no existing transaction record was found, initialize
	// to the transaction in the request header.
	if !ok {
		gogoproto.Merge(&txn, args.Txn)
	}
	if txn.Status == proto.PENDING {
		if txn.LastHeartbeat == nil {
			txn.LastHeartbeat = &proto.Timestamp{}
		}
		if txn.LastHeartbeat.Less(args.Header().Timestamp) {
			*txn.LastHeartbeat = args.Header().Timestamp
		}
		if err := engine.MVCCPutProto(batch, nil, key, proto.ZeroTimestamp, nil, &txn); err != nil {
			reply.SetGoError(err)
			return
		}
	}
	reply.Txn = &txn
}

// InternalPushTxn resolves conflicts between concurrent txns (or
// between a non-transactional reader or writer and a txn) in several
// ways depending on the statuses and priorities of the conflicting
// transactions. The InternalPushTxn operation is invoked by a
// "pusher" (the writer trying to abort a conflicting txn or the
// reader trying to push a conflicting txn's commit timestamp
// forward), who attempts to resolve a conflict with a "pushee"
// (args.PushTxn -- the pushee txn whose intent(s) caused the
// conflict).
//
// Txn already committed/aborted: If pushee txn is committed or
// aborted return success.
//
// Txn Timeout: If pushee txn entry isn't present or its LastHeartbeat
// timestamp isn't set, use PushTxn.Timestamp as LastHeartbeat. If
// current time - LastHeartbeat > 2 * DefaultHeartbeatInterval, then
// the pushee txn should be either pushed forward or aborted,
// depending on value of Request.Abort.
//
// Old Txn Epoch: If persisted pushee txn entry has a newer Epoch than
// PushTxn.Epoch, return success, as older epoch may be removed.
//
// Lower Txn Priority: If pushee txn has a lower priority than pusher,
// adjust pushee's persisted txn depending on value of args.Abort. If
// args.Abort is true, set txn.Status to ABORTED, and priority to one
// less than the pusher's priority and return success. If args.Abort
// is false, set txn.Timestamp to pusher's Timestamp + 1 (note that
// we use the pusher's Args.Timestamp, not Txn.Timestamp because the
// args timestamp can advance during the txn).
//
// Higher Txn Priority: If pushee txn has a higher priority than
// pusher, return TransactionPushError. Transaction will be retried
// with priority one less than the pushee's higher priority.
func (r *Range) InternalPushTxn(batch engine.Engine, args *proto.InternalPushTxnRequest, reply *proto.InternalPushTxnResponse) {
	if !bytes.Equal(args.Key, args.PusheeTxn.Key) {
		reply.SetGoError(util.Errorf("request key %q should match pushee's txn key %q", args.Key, args.PusheeTxn.Key))
		return
	}
	key := engine.MakeKey(engine.KeyLocalTransactionPrefix, args.PusheeTxn.Key, args.PusheeTxn.ID)

	// Fetch existing transaction if possible.
	existTxn := &proto.Transaction{}
	ok, err := engine.MVCCGetProto(batch, key, proto.ZeroTimestamp, nil, existTxn)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if ok {
		// Start with the persisted transaction record as final transaction.
		reply.PusheeTxn = gogoproto.Clone(existTxn).(*proto.Transaction)
		// Upgrade the epoch, timestamp and priority as necessary.
		if reply.PusheeTxn.Epoch < args.PusheeTxn.Epoch {
			reply.PusheeTxn.Epoch = args.PusheeTxn.Epoch
		}
		if reply.PusheeTxn.Timestamp.Less(args.PusheeTxn.Timestamp) {
			reply.PusheeTxn.Timestamp = args.PusheeTxn.Timestamp
		}
		if reply.PusheeTxn.Priority < args.PusheeTxn.Priority {
			reply.PusheeTxn.Priority = args.PusheeTxn.Priority
		}
	} else {
		// Some sanity checks for case where we don't find a transaction record.
		if args.PusheeTxn.LastHeartbeat != nil {
			reply.SetGoError(proto.NewTransactionStatusError(&args.PusheeTxn,
				"no txn persisted, yet intent has heartbeat"))
			return
		} else if args.PusheeTxn.Status != proto.PENDING {
			reply.SetGoError(proto.NewTransactionStatusError(&args.PusheeTxn,
				fmt.Sprintf("no txn persisted, yet intent has status %s", args.PusheeTxn.Status)))
			return
		}
		// The transaction doesn't exist yet on disk; use the supplied version.
		reply.PusheeTxn = gogoproto.Clone(&args.PusheeTxn).(*proto.Transaction)
	}

	// If already committed or aborted, return success.
	if reply.PusheeTxn.Status != proto.PENDING {
		// Trivial noop.
		return
	}
	// If we're trying to move the timestamp forward, and it's already
	// far enough forward, return success.
	if !args.Abort && args.Timestamp.Less(reply.PusheeTxn.Timestamp) {
		// Trivial noop.
		return
	}

	// pusherWins bool is true in the event the pusher prevails.
	var pusherWins bool

	// If there's no incoming transaction, the pusher is
	// non-transactional. We make a random priority, biased by
	// specified args.Header().UserPriority in this case.
	var priority int32
	if args.Txn != nil {
		priority = args.Txn.Priority
	} else {
		priority = proto.MakePriority(args.GetUserPriority())
	}

	// Check for txn timeout.
	if reply.PusheeTxn.LastHeartbeat == nil {
		reply.PusheeTxn.LastHeartbeat = &reply.PusheeTxn.Timestamp
	}
	// Compute heartbeat expiration.
	expiry := r.rm.Clock().Now()
	expiry.WallTime -= 2 * DefaultHeartbeatInterval.Nanoseconds()
	if reply.PusheeTxn.LastHeartbeat.Less(expiry) {
		log.V(1).Infof("pushing expired txn %s", reply.PusheeTxn)
		pusherWins = true
	} else if args.PusheeTxn.Epoch < reply.PusheeTxn.Epoch {
		// Check for an intent from a prior epoch.
		log.V(1).Infof("pushing intent from previous epoch for txn %s", reply.PusheeTxn)
		pusherWins = true
	} else if reply.PusheeTxn.Priority < priority ||
		(reply.PusheeTxn.Priority == priority && args.Txn.Timestamp.Less(reply.PusheeTxn.Timestamp)) {
		// Finally, choose based on priority; if priorities are equal, order by lower txn timestamp.
		log.V(1).Infof("pushing intent from txn with lower priority %s vs %d", reply.PusheeTxn, priority)
		pusherWins = true
	} else if reply.PusheeTxn.Isolation == proto.SNAPSHOT && !args.Abort {
		log.V(1).Infof("pushing timestamp for snapshot isolation txn")
		pusherWins = true
	}

	if !pusherWins {
		log.V(1).Infof("failed to push intent %s vs %s using priority=%d", reply.PusheeTxn, args.Txn, priority)
		reply.SetGoError(proto.NewTransactionPushError(args.Txn, reply.PusheeTxn))
		return
	}

	// Upgrade priority of pushed transaction to one less than pusher's.
	reply.PusheeTxn.UpgradePriority(priority - 1)

	// If aborting transaction, set new status and return success.
	if args.Abort {
		reply.PusheeTxn.Status = proto.ABORTED
	} else {
		// Otherwise, update timestamp to be one greater than the request's timestamp.
		reply.PusheeTxn.Timestamp = args.Timestamp
		reply.PusheeTxn.Timestamp.Logical++
	}

	// Persist the pushed transaction using zero timestamp for inline value.
	if err := engine.MVCCPutProto(batch, nil, key, proto.ZeroTimestamp, nil, reply.PusheeTxn); err != nil {
		reply.SetGoError(err)
		return
	}
}

// InternalResolveIntent updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. The range will return the current status for this
// transaction to the coordinator.
func (r *Range) InternalResolveIntent(batch engine.Engine, ms *engine.MVCCStats, args *proto.InternalResolveIntentRequest, reply *proto.InternalResolveIntentResponse) {
	if args.Txn == nil {
		reply.SetGoError(util.Errorf("no transaction specified to InternalResolveIntent"))
		return
	}
	if len(args.EndKey) == 0 || bytes.Equal(args.Key, args.EndKey) {
		reply.SetGoError(engine.MVCCResolveWriteIntent(batch, ms, args.Key, args.Txn))
	} else {
		_, err := engine.MVCCResolveWriteIntentRange(batch, ms, args.Key, args.EndKey, 0, args.Txn)
		reply.SetGoError(err)
	}
}

// InternalSnapshotCopy scans the key range specified by start key through
// end key up to some maximum number of results from the given snapshot_id.
// It will create a snapshot if snapshot_id is empty.
func (r *Range) InternalSnapshotCopy(e engine.Engine, args *proto.InternalSnapshotCopyRequest, reply *proto.InternalSnapshotCopyResponse) {
	if len(args.SnapshotID) == 0 {
		snapshotID, err := r.rm.CreateSnapshot()
		if err != nil {
			reply.SetGoError(err)
			return
		}
		args.SnapshotID = snapshotID
	}

	kvs, err := engine.ScanSnapshot(e, proto.EncodedKey(args.Key), proto.EncodedKey(args.EndKey), args.MaxResults, args.SnapshotID)
	if err != nil {
		reply.SetGoError(err)
		return
	}
	if len(kvs) == 0 {
		err = e.ReleaseSnapshot(args.SnapshotID)
	}

	reply.Rows = kvs
	reply.SnapshotID = args.SnapshotID
	reply.SetGoError(err)
}

// InternalMerge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used by
// Cockroach for the efficient accumulation of certain values. Due to the
// difficulty of making these operations transactional, merges are not currently
// exposed directly to clients. Merged values are explicitly not MVCC data.
func (r *Range) InternalMerge(batch engine.Engine, ms *engine.MVCCStats, args *proto.InternalMergeRequest, reply *proto.InternalMergeResponse) {
	err := engine.MVCCMerge(batch, ms, args.Key, args.Value)
	reply.SetGoError(err)
}

// splitTrigger is called on a successful commit of an AdminSplit
// transaction. It copies the response cache for the new range and
// recomputes stats for both the existing, updated range and the new
// range.
func (r *Range) splitTrigger(batch engine.Engine, split *proto.SplitTrigger) error {
	if !bytes.Equal(r.Desc.StartKey, split.UpdatedDesc.StartKey) ||
		!bytes.Equal(r.Desc.EndKey, split.NewDesc.EndKey) {
		return util.Errorf("range does not match splits: %q-%q + %q-%q != %q-%q", split.UpdatedDesc.StartKey,
			split.UpdatedDesc.EndKey, split.NewDesc.StartKey, split.NewDesc.EndKey, r.Desc.StartKey, r.Desc.EndKey)
	}

	// Compute stats for new range.
	ms, err := engine.MVCCComputeStats(r.rm.Engine(), split.NewDesc.StartKey, split.NewDesc.EndKey)
	if err != nil {
		return util.Errorf("unable to compute stats for new range after split: %s", err)
	}
	ms.SetStats(batch, split.NewDesc.RaftID, 0)
	// Compute stats for updated range.
	ms, err = engine.MVCCComputeStats(r.rm.Engine(), split.UpdatedDesc.StartKey, split.UpdatedDesc.EndKey)
	if err != nil {
		return util.Errorf("unable to compute stats for updated range after split: %s", err)
	}
	ms.SetStats(batch, r.Desc.RaftID, 0)

	// Initialize the new range's response cache by copying the original's.
	if err = r.respCache.CopyInto(batch, split.NewDesc.RaftID); err != nil {
		return util.Errorf("unable to copy response cache to new split range: %s", err)
	}

	// Add the new split range to the store. This step atomically
	// updates the EndKey of the updated range and also adds the
	// new range to the store's range map.
	newRng := NewRange(&split.NewDesc, r.rm)
	// Write-lock the mutex to protect Desc, as SplitRange will modify
	// Desc.EndKey.
	r.Lock()
	defer r.Unlock()
	return r.rm.SplitRange(r, newRng)
}

// mergeTrigger is called on a successful commit of an AdminMerge
// transaction. It recomputes stats for the receiving range.
func (r *Range) mergeTrigger(batch engine.Engine, merge *proto.MergeTrigger) error {
	if !bytes.Equal(r.Desc.StartKey, merge.UpdatedDesc.StartKey) {
		return util.Errorf("range and updated range start keys do not match: %q != %q",
			r.Desc.StartKey, merge.UpdatedDesc.StartKey)
	}

	if !r.Desc.EndKey.Less(merge.UpdatedDesc.EndKey) {
		return util.Errorf("range end key is not less than the post merge end key: %q < %q",
			r.Desc.EndKey, merge.UpdatedDesc.StartKey)
	}

	if merge.SubsumedRaftID <= 0 {
		return util.Errorf("subsumed raft ID must be provided: %d", merge.SubsumedRaftID)
	}

	// Copy the subsumed range's response cache to the subsuming one.
	if err := r.respCache.CopyFrom(batch, merge.SubsumedRaftID); err != nil {
		return util.Errorf("unable to copy response cache to new split range: %s", err)
	}

	// Compute stats for updated range.
	ms, err := engine.MVCCComputeStats(r.rm.Engine(), merge.UpdatedDesc.StartKey,
		merge.UpdatedDesc.EndKey)
	if err != nil {
		return util.Errorf("unable to compute stats for the range after merge: %s", err)
	}
	ms.SetStats(batch, r.Desc.RaftID, 0)

	// Write-lock the mutex to protect Desc, as MergeRange will modify
	// Desc.EndKey.
	r.Lock()
	defer r.Unlock()
	return r.rm.MergeRange(r, merge.UpdatedDesc.EndKey, merge.SubsumedRaftID)
}

// AdminSplit divides the range into into two ranges, using either
// args.SplitKey (if provided) or an internally computed key that aims to
// roughly equipartition the range by size. The split is done inside of
// a distributed txn which writes updated and new range descriptors, and
// updates the range addressing metadata. The handover of responsibility for
// the reassigned key range is carried out seamlessly through a split trigger
// carried out as part of the commit of that transaction.
func (r *Range) AdminSplit(args *proto.AdminSplitRequest, reply *proto.AdminSplitResponse) {
	// Only allow a single split per range at a time.
	if !atomic.CompareAndSwapInt32(&r.splitting, int32(0), int32(1)) {
		reply.SetGoError(util.Errorf("already splitting range %d", r.Desc.RaftID))
		return
	}
	defer func() { atomic.StoreInt32(&r.splitting, int32(0)) }()

	// Determine split key if not provided with args. This scan is
	// allowed to be relatively slow because admin commands don't block
	// other commands.
	splitKey := proto.Key(args.SplitKey)
	if len(splitKey) == 0 {
		snapshotID, err := r.rm.CreateSnapshot()
		if err != nil {
			reply.SetGoError(util.Errorf("unable to create snapshot: %s", err))
			return
		}
		splitKey, err = engine.MVCCFindSplitKey(r.rm.Engine(), r.Desc.RaftID, r.Desc.StartKey, r.Desc.EndKey, snapshotID)
		if releaseErr := r.rm.Engine().ReleaseSnapshot(snapshotID); releaseErr != nil {
			log.Errorf("unable to release snapshot: %s", releaseErr)
		}
		if err != nil {
			reply.SetGoError(util.Errorf("unable to determine split key: %s", err))
			return
		}
	}

	// Verify some properties of split key.
	if !r.ContainsKey(splitKey) {
		reply.SetGoError(proto.NewRangeKeyMismatchError(splitKey, splitKey, r.Desc))
		return
	}
	if !engine.IsValidSplitKey(splitKey) {
		reply.SetGoError(util.Errorf("cannot split range at key %q", splitKey))
		return
	}
	if splitKey.Equal(r.Desc.StartKey) || splitKey.Equal(r.Desc.EndKey) {
		reply.SetGoError(util.Errorf("range has already been split by key %q", splitKey))
		return
	}

	// Create new range descriptor with newly-allocated replica IDs and Raft IDs.
	newDesc, err := r.rm.NewRangeDescriptor(splitKey, r.Desc.EndKey, r.Desc.Replicas)
	if err != nil {
		reply.SetGoError(util.Errorf("unable to allocate new range descriptor: %s", err))
		return
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *r.Desc
	updatedDesc.EndKey = splitKey

	log.Infof("initiating a split of range %d %q-%q at key %q", r.Desc.RaftID,
		proto.Key(r.Desc.StartKey), proto.Key(r.Desc.EndKey), splitKey)

	txnOpts := &client.TransactionOptions{
		Name: fmt.Sprintf("split range %d at %q", r.Desc.RaftID, splitKey),
	}
	if err = r.rm.DB().RunTransaction(txnOpts, func(txn *client.KV) error {
		// Create range descriptor for second half of split.
		// Note that this put must go first in order to locate the
		// transaction record on the correct range.
		if err := txn.PreparePutProto(engine.RangeDescriptorKey(newDesc.StartKey), newDesc); err != nil {
			return err
		}
		// Update existing range descriptor for first half of split.
		if err := txn.PreparePutProto(engine.RangeDescriptorKey(updatedDesc.StartKey), &updatedDesc); err != nil {
			return err
		}
		// Update range descriptor addressing record(s).
		if err := SplitRangeAddressing(txn, newDesc, &updatedDesc); err != nil {
			return err
		}
		// End the transaction manually, instead of letting RunTransaction
		// loop do it, in order to provide a split trigger.
		return txn.Call(proto.EndTransaction, &proto.EndTransactionRequest{
			RequestHeader: proto.RequestHeader{Key: args.Key},
			Commit:        true,
			SplitTrigger: &proto.SplitTrigger{
				UpdatedDesc: updatedDesc,
				NewDesc:     *newDesc,
			},
		}, &proto.EndTransactionResponse{})
	}); err != nil {
		reply.SetGoError(util.Errorf("split at key %q failed: %s", splitKey, err))
	}
}

// intersectReplicaSets is used in AdminMerge to ensure that the ranges are
// all colocated on the same set of replicas.
func intersectReplicaSets(a, b []proto.Replica) bool {
	if len(a) != len(b) {
		return false
	}

	set := make(map[int32]int)
	for _, replica := range a {
		set[replica.StoreID]++
	}

	for _, replica := range b {
		set[replica.StoreID]--
	}

	for _, value := range set {
		if value != 0 {
			return false
		}
	}

	return true
}

// AdminMerge extends the range to subsume the range that comes next in
// the key space.  The range being subsumed is provided in args.SubsumedRange.
// The EndKey of the the subsuming range must equal the start key of the
// range being subsumed.  The merge is performed inside of a distributed
// transaction which writes the updated range descriptor for the subsuming range
// and deletes the range descriptor for the subsumed one.  It also updates the
// range addressing metadata.  The handover of responsibility for
// the reassigned key range is carried out seamlessly through a merge trigger
// carried out as part of the commit of that transaction.
// A merge requires that the two ranges are colocated on the same set of replicas.
func (r *Range) AdminMerge(args *proto.AdminMergeRequest, reply *proto.AdminMergeResponse) {
	// Only allow a single split/merge per range at a time.
	if !atomic.CompareAndSwapInt32(&r.splitting, int32(0), int32(1)) {
		reply.SetGoError(util.Errorf("already splitting/merging range %d", r.Desc.RaftID))
		return
	}
	defer func() { atomic.StoreInt32(&r.splitting, int32(0)) }()

	subsumedDesc := args.SubsumedRange

	// Ensure that the subsumedRange is not the first range.
	if subsumedDesc.StartKey.Equal(engine.KeyMin) {
		reply.SetGoError(util.Error("Cannot subsume the first range"))
		return
	}

	// Ensure the calling range exists.  This will only happen if KeyMin is
	// not zero.
	if len(subsumedDesc.StartKey) == 0 {
		reply.SetGoError(util.Error("No subsumed range specified"))
		return
	}

	// Make sure the range being subsumed follows this one.
	if !bytes.Equal(r.Desc.EndKey, subsumedDesc.StartKey) {
		reply.SetGoError(util.Errorf("Ranges that are not adjacent cannot be merged, %d = %d",
			r.Desc.EndKey, subsumedDesc.StartKey))
		return
	}

	// Ensure that both ranges are colocated by intersecting the store ids from
	// their replicas.
	if !intersectReplicaSets(subsumedDesc.GetReplicas(), r.Desc.GetReplicas()) {
		reply.SetGoError(util.Error("The two ranges replicas are not colocated"))
	}

	// Init updated version of existing range descriptor.
	updatedDesc := *r.Desc
	updatedDesc.EndKey = subsumedDesc.EndKey

	log.Infof("initiating a merge of range %d %q-%q into range %d %q-%q",
		subsumedDesc.RaftID, proto.Key(subsumedDesc.StartKey), proto.Key(subsumedDesc.EndKey),
		r.Desc.RaftID, r.Desc.StartKey, r.Desc.EndKey)

	txnOpts := &client.TransactionOptions{
		Name: fmt.Sprintf("merge range %d into %d", subsumedDesc.RaftID, r.Desc.RaftID),
	}
	if err := r.rm.DB().RunTransaction(txnOpts, func(txn *client.KV) error {
		// Update the range descriptor for the receiving range
		if err := txn.PreparePutProto(engine.RangeDescriptorKey(updatedDesc.StartKey), &updatedDesc); err != nil {
			return err
		}

		// Remove the range descriptor for the deleted range
		deleteResponse := &proto.DeleteResponse{}
		txn.Prepare(proto.Delete, proto.DeleteArgs(engine.RangeDescriptorKey(subsumedDesc.StartKey)),
			deleteResponse)
		if deleteResponse.Error != nil {
			return util.Error(deleteResponse.Error)
		}

		if err := MergeRangeAddressing(txn, r.Desc, &updatedDesc); err != nil {
			return err
		}

		// End the transaction manually instead of letting RunTransaction
		// loop do it, in order to provide a merge trigger.
		return txn.Call(proto.EndTransaction, &proto.EndTransactionRequest{
			RequestHeader: proto.RequestHeader{Key: args.Key},
			Commit:        true,
			MergeTrigger: &proto.MergeTrigger{
				UpdatedDesc:    updatedDesc,
				SubsumedRaftID: subsumedDesc.RaftID,
			},
		}, &proto.EndTransactionResponse{})
	}); err != nil {
		reply.SetGoError(util.Errorf("merge of range %d into %d failed: %s",
			subsumedDesc.RaftID, r.Desc.RaftID, err))
	}
}
