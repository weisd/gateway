package model

import (
	"context"
	"fmt"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/coreos/etcd/clientv3"
)

var _ Store = EtcdStoreV3{}

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

	watchCh chan *etcd.Response
	evtCh   chan *Evt

	watchMethodMapping map[EvtSrc]func(EvtType, *etcd.Response) *Evt
}

// NewEtcdStoreV3 create a etcd store
func NewEtcdStoreV3(etcdAddrs []string, prefix string) (Store, error) {
	store := EtcdStore{
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
		watchMethodMapping: make(map[EvtSrc]func(EvtType, *etcd.Response) *Evt),
	}

	cli, err := initEctdClient(etcdAddr)
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
		return nil, errors.Wrap(err, "")
	}

	return client, nil
}

func (e EtcdStoreV3) SaveBind(bind *Bind) error {
	return nil
}
func (e EtcdStoreV3) UnBind(bind *Bind) error {
	return nil
}
func (e EtcdStoreV3) GetBinds() ([]*Bind, error) {
	return nil, nil
}

func (e EtcdStoreV3) SaveCluster(cluster *Cluster) error {
	return nil
}
func (e EtcdStoreV3) UpdateCluster(cluster *Cluster) error {
	return nil
}
func (e EtcdStoreV3) DeleteCluster(name string) error {
	return nil
}
func (e EtcdStoreV3) GetClusters() ([]*Cluster, error) {
	return nil, nil
}
func (e EtcdStoreV3) GetCluster(clusterName string) (*Cluster, error) {
	return nil, nil
}

func (e EtcdStoreV3) SaveServer(svr *Server) error {
	return nil
}
func (e EtcdStoreV3) UpdateServer(svr *Server) error {
	return nil
}
func (e EtcdStoreV3) DeleteServer(addr string) error {
	return nil
}
func (e EtcdStoreV3) GetServers() ([]*Server, error) {
	return nil, nil
}
func (e EtcdStoreV3) GetServer(serverAddr string) (*Server, error) {
	return nil, nil
}

func (e EtcdStoreV3) SaveAPI(api *API) error {
	return nil
}
func (e EtcdStoreV3) UpdateAPI(api *API) error {
	return nil
}
func (e EtcdStoreV3) DeleteAPI(url string, method string) error {
	return nil
}
func (e EtcdStoreV3) GetAPIs() ([]*API, error) {
	return nil, nil
}
func (e EtcdStoreV3) GetAPI(url string, method string) (*API, error) {
	return nil, nil
}

func (e EtcdStoreV3) SaveRouting(routing *Routing) error {
	return nil
}
func (e EtcdStoreV3) GetRoutings() ([]*Routing, error) {
	return nil, nil
}

func (e EtcdStoreV3) Watch(evtCh chan *Evt, stopCh chan bool) error {
	return nil
}

func (e EtcdStoreV3) Clean() error {
	return nil
}
func (e EtcdStoreV3) GC() error {
	return nil
}

func (e EtcdStoreV3) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

func (e EtcdStoreV3) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.cli.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(s.cli).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("embed-ectd: read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, errors.Wrap(err, "")
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("embed-ectd: read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (e EtcdStoreV3) save(key, value string) error {
	resp, err := s.txn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return errors.Wrap(err, "")
	}

	if !resp.Succeeded {
		return errors.Wrap(errTxnFailed, "")
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

	return resp, errors.Wrap(err, "")
}
