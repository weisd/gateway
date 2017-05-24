package model

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

var (
	_            Store = EtcdStoreV3{}
	errTxnFailed       = errors.New("etcd: failed to commit transaction")
	ErrHasBind         = errors.New("Has bind info, can not delete")
)

const (
	// DefaultTimeout default timeout
	DefaultTimeout = time.Second * 3
	// DefaultRequestTimeout default request timeout
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSlowRequestTime default slow request time
	DefaultSlowRequestTime = time.Second * 1
)

// EtcdStoreV3 Etcd store with clientv3
type EtcdStoreV3 struct {
	prefix            string
	clustersDir       string
	serversDir        string
	bindsDir          string
	apisDir           string
	proxiesDir        string
	routingsDir       string
	deleteServersDir  string
	deleteClustersDir string
	deleteAPIsDir     string

	cli *clientv3.Client

	watchCh   <-chan *clientv3.Event
	leaseChan <-chan *clientv3.LeaseKeepAliveResponse
	evtCh     chan *Evt

	watchMethodMapping map[EvtSrc]func(EvtType, *clientv3.Event) *Evt
}

// NewEtcdStoreV3 create a etcd store
func NewEtcdStoreV3(etcdAddrs []string, prefix string) (Store, error) {
	if prefix[0] != '/' {
		prefix = "/" + prefix
	}

	store := EtcdStoreV3{
		prefix:            prefix,
		clustersDir:       fmt.Sprintf("%s/clusters", prefix),
		serversDir:        fmt.Sprintf("%s/servers", prefix),
		bindsDir:          fmt.Sprintf("%s/binds", prefix),
		apisDir:           fmt.Sprintf("%s/apis", prefix),
		proxiesDir:        fmt.Sprintf("%s/proxy", prefix),
		routingsDir:       fmt.Sprintf("%s/routings", prefix),
		deleteServersDir:  fmt.Sprintf("%s/delete/servers", prefix),
		deleteClustersDir: fmt.Sprintf("%s/delete/clusters", prefix),
		deleteAPIsDir:     fmt.Sprintf("%s/delete/apis", prefix),

		// cli:                initEctdClient(etcdAddrs),
		watchMethodMapping: make(map[EvtSrc]func(EvtType, *clientv3.Event) *Evt),
	}

	cli, err := initEctdClient(etcdAddrs)
	if err != nil {
		return nil, err
	}

	store.cli = cli

	store.init()
	return store, nil
}

func initEctdClient(etcdAddrs []string) (*clientv3.Client, error) {

	log.Infof("bootstrap: create etcd v3 client, endpoints=<%v>", etcdAddrs)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdAddrs,
		DialTimeout: DefaultTimeout,
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (e EtcdStoreV3) doWatchWithCluster(evtType EvtType, ev *clientv3.Event) *Evt {

	cluster := UnMarshalCluster([]byte(ev.Kv.Value))

	return &Evt{
		Src:   EventSrcCluster,
		Type:  evtType,
		Key:   strings.Replace(string(ev.Kv.Key), fmt.Sprintf("%s/", e.clustersDir), "", 1),
		Value: cluster,
	}
}

func (e EtcdStoreV3) doWatchWithServer(evtType EvtType, ev *clientv3.Event) *Evt {

	server := UnMarshalServer([]byte(ev.Kv.Value))

	return &Evt{
		Src:   EventSrcServer,
		Type:  evtType,
		Key:   strings.Replace(string(ev.Kv.Key), fmt.Sprintf("%s/", e.serversDir), "", 1),
		Value: server,
	}
}

func (e EtcdStoreV3) doWatchWithBind(evtType EvtType, ev *clientv3.Event) *Evt {

	key := strings.Replace(string(ev.Kv.Key), fmt.Sprintf("%s/", e.bindsDir), "", 1)
	infos := strings.SplitN(key, "-", 2)

	return &Evt{
		Src:  EventSrcBind,
		Type: evtType,
		Key:  string(ev.Kv.Key),
		Value: &Bind{
			ServerAddr:  infos[0],
			ClusterName: infos[1],
		},
	}
}

func (e EtcdStoreV3) doWatchWithAPI(evtType EvtType, ev *clientv3.Event) *Evt {

	api := UnMarshalAPI([]byte(ev.Kv.Value))
	value := strings.Replace(string(ev.Kv.Key), fmt.Sprintf("%s/", e.apisDir), "", 1)

	return &Evt{
		Src:   EventSrcAPI,
		Type:  evtType,
		Key:   value,
		Value: api,
	}
}

func (e EtcdStoreV3) doWatchWithRouting(evtType EvtType, ev *clientv3.Event) *Evt {

	routing := UnMarshalRouting([]byte(ev.Kv.Value))

	return &Evt{
		Src:   EventSrcRouting,
		Type:  evtType,
		Key:   strings.Replace(string(ev.Kv.Key), fmt.Sprintf("%s/", e.routingsDir), "", 1),
		Value: routing,
	}
}

func (e EtcdStoreV3) init() {
	e.watchMethodMapping[EventSrcBind] = e.doWatchWithBind
	e.watchMethodMapping[EventSrcServer] = e.doWatchWithServer
	e.watchMethodMapping[EventSrcCluster] = e.doWatchWithCluster
	e.watchMethodMapping[EventSrcAPI] = e.doWatchWithAPI
	e.watchMethodMapping[EventSrcRouting] = e.doWatchWithRouting
}

func (e EtcdStoreV3) SaveBind(bind *Bind) error {

	key := fmt.Sprintf("%s/%s", e.bindsDir, bind.ToString())

	err := e.save(key, "")
	if err != nil {
		return err
	}

	// update server bind info
	svr, err := e.GetServer(bind.ServerAddr)
	if err != nil {
		return err
	}

	svr.AddBind(bind)

	err = e.doUpdateServer(svr)
	if err != nil {
		return err
	}

	// update cluster bind info
	c, err := e.GetCluster(bind.ClusterName)
	if err != nil {
		return err
	}

	c.AddBind(bind)

	return e.doUpdateCluster(c)

}
func (e EtcdStoreV3) UnBind(bind *Bind) error {
	key := fmt.Sprintf("%s/%s", e.bindsDir, bind.ToString())

	err := e.delete(key, true)
	if err != nil {
		return err
	}

	svr, err := e.GetServer(bind.ServerAddr)
	if err != nil {
		return err
	}

	svr.RemoveBind(bind.ClusterName)

	err = e.doUpdateServer(svr)
	if err != nil {
		return err
	}

	c, err := e.GetCluster(bind.ClusterName)
	if err != nil {
		return err
	}

	c.RemoveBind(bind.ServerAddr)
	return e.doUpdateCluster(c)
}
func (e EtcdStoreV3) GetBinds() ([]*Bind, error) {

	resp, err := e.get(e.bindsDir, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if nil != err {
		return nil, err
	}

	l := len(resp.Kvs)
	values := make([]*Bind, l)

	for i := 0; i < l; i++ {
		key := strings.Replace(string(resp.Kvs[i].Key), fmt.Sprintf("%s/", e.bindsDir), "", 1)
		infos := strings.SplitN(key, "-", 2)

		values[i] = &Bind{
			ServerAddr:  infos[0],
			ClusterName: infos[1],
		}
	}

	return values, nil
}

func (e EtcdStoreV3) SaveCluster(cluster *Cluster) error {
	key := fmt.Sprintf("%s/%s", e.clustersDir, cluster.Name)
	return e.save(key, string(cluster.Marshal()))
}

func (e EtcdStoreV3) UpdateCluster(cluster *Cluster) error {

	old, err := e.GetCluster(cluster.Name)

	if nil != err {
		return err
	}

	old.updateFrom(cluster)

	return e.doUpdateCluster(old)
}

func (e *EtcdStoreV3) doUpdateCluster(cluster *Cluster) error {
	key := fmt.Sprintf("%s/%s", e.clustersDir, cluster.Name)
	return e.save(key, string(cluster.Marshal()))
}

func (e EtcdStoreV3) DeleteCluster(name string) error {

	c, err := e.GetCluster(name)
	if err != nil {
		return err
	}

	if c.HasBind() {
		return ErrHasBind
	}

	return e.deleteKey(name, e.clustersDir, e.deleteClustersDir)
}

func (e EtcdStoreV3) deleteKey(value, prefixKey, cacheKey string) error {
	deleteKey := fmt.Sprintf("%s/%s", cacheKey, value)
	err := e.save(deleteKey, value)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%s", prefixKey, value)
	err = e.delete(key, true)
	if nil != err {
		return err
	}

	err = e.delete(deleteKey, true)

	return err
}

func (e EtcdStoreV3) GetClusters() ([]*Cluster, error) {
	rsp, err := e.get(e.clustersDir, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if nil != err {
		return nil, err
	}

	l := len(rsp.Kvs)
	clusters := make([]*Cluster, l)

	for i := 0; i < l; i++ {
		clusters[i] = UnMarshalCluster(rsp.Kvs[i].Value)
	}

	return clusters, nil
}
func (e EtcdStoreV3) GetCluster(clusterName string) (*Cluster, error) {
	key := fmt.Sprintf("%s/%s", e.clustersDir, clusterName)
	rsp, err := e.get(key)
	if nil != err {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return UnMarshalCluster([]byte{}), nil
	}

	return UnMarshalCluster(rsp.Kvs[0].Value), nil
}

func (e EtcdStoreV3) SaveServer(svr *Server) error {
	key := fmt.Sprintf("%s/%s", e.serversDir, svr.Addr)
	return e.save(key, string(svr.Marshal()))
}
func (e EtcdStoreV3) UpdateServer(svr *Server) error {
	old, err := e.GetServer(svr.Addr)

	if nil != err {
		return err
	}

	old.updateFrom(svr)

	return e.doUpdateServer(old)
}

func (e *EtcdStoreV3) doUpdateServer(svr *Server) error {
	key := fmt.Sprintf("%s/%s", e.serversDir, svr.Addr)
	return e.save(key, string(svr.Marshal()))
}

func (e EtcdStoreV3) DeleteServer(addr string) error {
	svr, err := e.GetServer(addr)
	if err != nil {
		return err
	}

	if svr.HasBind() {
		return ErrHasBind
	}

	return e.deleteKey(addr, e.serversDir, e.deleteServersDir)
}
func (e EtcdStoreV3) GetServers() ([]*Server, error) {
	rsp, err := e.get(e.serversDir, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

	if nil != err {
		return nil, err
	}

	l := len(rsp.Kvs)
	servers := make([]*Server, l)

	for i := 0; i < l; i++ {
		servers[i] = UnMarshalServer([]byte(rsp.Kvs[i].Value))
	}

	return servers, nil
}
func (e EtcdStoreV3) GetServer(serverAddr string) (*Server, error) {
	key := fmt.Sprintf("%s/%s", e.serversDir, serverAddr)
	rsp, err := e.get(key)

	if nil != err {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return UnMarshalServer([]byte{}), nil
	}

	return UnMarshalServer(rsp.Kvs[0].Value), nil
}

func (e EtcdStoreV3) SaveAPI(api *API) error {
	key := fmt.Sprintf("%s/%s", e.apisDir, getAPIKey(api.URL, api.Method))
	return e.save(key, string(api.Marshal()))
}
func (e EtcdStoreV3) UpdateAPI(api *API) error {
	key := fmt.Sprintf("%s/%s", e.apisDir, getAPIKey(api.URL, api.Method))
	return e.save(key, string(api.Marshal()))
}
func (e EtcdStoreV3) DeleteAPI(url string, method string) error {
	return e.deleteKey(getAPIKey(url, method), e.apisDir, e.deleteAPIsDir)
}

func (e EtcdStoreV3) deleteAPIGC(key string) error {
	return e.deleteKey(key, e.apisDir, e.deleteAPIsDir)
}

func (e EtcdStoreV3) GetAPIs() ([]*API, error) {
	rsp, err := e.get(e.apisDir, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

	if nil != err {
		return nil, err
	}

	l := len(rsp.Kvs)
	apis := make([]*API, l)

	for i := 0; i < l; i++ {
		apis[i] = UnMarshalAPI([]byte(rsp.Kvs[i].Value))
	}

	return apis, nil
}
func (e EtcdStoreV3) GetAPI(url string, method string) (*API, error) {
	key := fmt.Sprintf("%s/%s", e.apisDir, getAPIKey(url, method))
	rsp, err := e.get(key)

	if nil != err {
		return nil, err
	}

	if len(rsp.Kvs) == 0 {
		return UnMarshalAPI([]byte{}), nil
	}

	return UnMarshalAPI([]byte(rsp.Kvs[0].Value)), nil
}

func (e EtcdStoreV3) SaveRouting(routing *Routing) error {
	key := fmt.Sprintf("%s/%s", e.routingsDir, routing.ID)

	opts := []clientv3.OpOption{}
	if routing.deadline > 0 {
		lessor := clientv3.NewLease(e.cli)
		defer lessor.Close()

		start := time.Now()
		ctx, cancel := context.WithTimeout(e.cli.Ctx(), DefaultRequestTimeout)
		leaseResp, err := lessor.Grant(ctx, routing.deadline)
		cancel()

		if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
			log.Warnf("embed-ectd: lessor grants too slow, cost=<%s>", cost)
		}

		if err != nil {
			return err
		}

		opts = append(opts, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))
	}
	return e.save(key, string(routing.Marshal()), opts...)

}
func (e EtcdStoreV3) GetRoutings() ([]*Routing, error) {
	rsp, err := e.get(e.routingsDir, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

	if nil != err {
		return nil, err
	}

	l := len(rsp.Kvs)
	routings := make([]*Routing, l)

	for i := 0; i < l; i++ {
		routings[i] = UnMarshalRouting(rsp.Kvs[i].Value)
	}

	return routings, nil
}

func (e EtcdStoreV3) Watch(evtCh chan *Evt, stopCh chan bool) error {
	e.watchCh = make(<-chan *clientv3.Event)
	e.evtCh = evtCh

	go func() {
		for {
			e.doWatch()

			log.Error("etcd: watch server close")
			// sleep ?
		}

	}()

	return nil
}

func (e EtcdStoreV3) doWatch() {
	watcher := clientv3.NewWatcher(e.cli)
	defer watcher.Close()

	ctx := e.cli.Ctx()
	rch := watcher.Watch(ctx, e.prefix, clientv3.WithPrefix())
	for {

		select {
		case wresp := <-rch:
			if wresp.Canceled {
				return
			}

			e.doReceiveEvent(wresp.Events)

		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

func (e EtcdStoreV3) doReceiveEvent(evts []*clientv3.Event) {
	for _, ev := range evts {
		log.Debugf("doReceiveEvent %s %q : %q, pre %v \n", ev.Type, string(ev.Kv.Key), ev.Kv.Value, ev.PrevKv)

		var evtSrc EvtSrc
		var evtType EvtType

		key := string(ev.Kv.Key)

		switch {
		case strings.HasPrefix(key, e.clustersDir):
			evtSrc = EventSrcCluster
		case strings.HasPrefix(key, e.serversDir):
			evtSrc = EventSrcServer
		case strings.HasPrefix(key, e.bindsDir):
			evtSrc = EventSrcBind
		case strings.HasPrefix(key, e.apisDir):
			evtSrc = EventSrcAPI
		case strings.HasPrefix(key, e.routingsDir):
			evtSrc = EventSrcRouting
		default:
			log.Warnf("doReceiveEvent unknow key %s", key)
			continue
		}

		log.Debug(evtSrc, evtType)

		switch {
		case ev.IsCreate():
			evtType = EventTypeNew
		case ev.IsModify():
			evtType = EventTypeUpdate

		case ev.Type == mvccpb.DELETE:
			evtType = EventTypeDelete
		default:
			log.Warnf("doReceiveEvent evtType key %s", ev.Type.String())
			continue

		}

		e.evtCh <- e.watchMethodMapping[evtSrc](evtType, ev)

	}

}

func (e EtcdStoreV3) Clean() error {
	return e.delete(e.prefix, true)

}
func (e EtcdStoreV3) GC() error {
	// process not complete delete opts
	err := e.gcDir(e.deleteServersDir, e.DeleteServer)

	if nil != err {
		return err
	}

	err = e.gcDir(e.deleteClustersDir, e.DeleteCluster)

	if nil != err {
		return err
	}

	err = e.gcDir(e.deleteAPIsDir, e.deleteAPIGC)

	if nil != err {
		return err
	}

	return nil
}

func (e EtcdStoreV3) gcDir(dir string, fn func(value string) error) error {
	rsp, err := e.get(dir, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, node := range rsp.Kvs {
		err = fn(string(node.Value))
		if err != nil {
			return err
		}
	}

	return nil
}

func (e EtcdStoreV3) txn() clientv3.Txn {
	return newSlowLogTxn(e.cli)
}

func (e EtcdStoreV3) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(e.cli.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(e.cli).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("embed-ectd: read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("embed-ectd: read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (e EtcdStoreV3) save(key, value string, opts ...clientv3.OpOption) error {
	resp, err := e.txn().Then(clientv3.OpPut(key, value, opts...)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (e EtcdStoreV3) delete(key string, withPfreix bool) error {

	ops := []clientv3.OpOption{}

	if withPfreix {
		ops = append(ops, clientv3.WithPrefix())
	}
	resp, err := e.txn().Then(clientv3.OpDelete(key, ops...)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Now().Sub(start)
	if cost > DefaultSlowRequestTime {
		log.Warn("embed-ectd: txn runs too slow, resp=<%v> cost=<%s> errors:\n %+v",
			resp,
			cost,
			err)
	}

	return resp, err
}

func getAPIKey(apiURL, method string) string {
	key := fmt.Sprintf("%s-%s", apiURL, method)
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func parseAPIKey(key string) (url string, method string) {
	raw := decodeAPIKey(key)
	splits := strings.SplitN(raw, "-", 2)
	url = splits[0]
	method = splits[1]

	return url, method
}

func decodeAPIKey(key string) string {
	raw, _ := base64.RawURLEncoding.DecodeString(key)
	return string(raw)
}
