package model

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/gateway/pkg/util"
	"github.com/toolkits/net"
)

var (
	_ Register = EtcdStoreV3{}

	// LeaseTTL ttl
	LeaseTTL int64 = 5
)

// Registry Registry clientv3
func (e EtcdStoreV3) Registry(proxyInfo *ProxyInfo) error {

	proxyInfo.Conf.Addr = util.ConvertIP(proxyInfo.Conf.Addr)
	proxyInfo.Conf.MgrAddr = util.ConvertIP(proxyInfo.Conf.MgrAddr)

	go func() {
		for {
			if err := e.doRegister(proxyInfo); err != nil {
				log.Error(err)
			}
		}

	}()

	return nil
}

func (e EtcdStoreV3) doRegister(proxyInfo *ProxyInfo) error {
	lessor := clientv3.NewLease(e.cli)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(e.cli.Ctx(), DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, LeaseTTL)
	cancel()

	if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
		log.Warnf("embed-ectd: lessor grants too slow, cost=<%s>", cost)
	}

	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%s", e.proxiesDir, proxyInfo.Conf.Addr)

	resp, err := e.txn().
		// If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, proxyInfo.Marshal(), clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return err
	}

	log.Infof("etcd: register commit resp %v", resp)

	if !resp.Succeeded {
		return errors.New("register proxy failed")
	}

	leaseChan, err := lessor.KeepAlive(e.cli.Ctx(), clientv3.LeaseID(leaseResp.ID))

	for {
		select {
		case _, ok := <-leaseChan:
			if !ok {
				log.Info("etcd: channel that keep alive for proxy lease is closed")
				return nil
			}

			// log.Debugf("lease %v", l)
		case <-e.cli.Ctx().Done():
			return errors.New("etcd: server closed")
		}
	}

}

func (e EtcdStoreV3) GetProxies() ([]*ProxyInfo, error) {
	rsp, err := e.get(e.proxiesDir, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))

	if nil != err {
		return nil, err
	}

	fmt.Printf("GetProxies %#v\n", rsp.Kvs)

	l := len(rsp.Kvs)
	proxies := make([]*ProxyInfo, l)

	for i := 0; i < l; i++ {
		proxies[i] = UnMarshalProxyInfo(rsp.Kvs[i].Value)
	}

	return proxies, nil
}

func (e EtcdStoreV3) ChangeLogLevel(proxyAddr string, level string) error {
	rpcClient, err := net.RpcClient("tcp", proxyAddr, time.Second*5)
	if err != nil {
		return err
	}

	req := SetLogReq{
		Level: level,
	}

	rsp := &SetLogRsp{
		Code: 0,
	}

	return rpcClient.Call("Manager.SetLogLevel", req, rsp)
}

func (e EtcdStoreV3) AddAnalysisPoint(proxyAddr, serverAddr string, secs int) error {
	rpcClient, err := net.RpcClient("tcp", proxyAddr, time.Second*5)
	if err != nil {
		return err
	}

	req := AddAnalysisPointReq{
		Addr: serverAddr,
		Secs: secs,
	}

	rsp := &AddAnalysisPointRsp{
		Code: 0,
	}

	return rpcClient.Call("Manager.AddAnalysisPoint", req, rsp)
}

func (e EtcdStoreV3) GetAnalysisPoint(proxyAddr, serverAddr string, secs int) (*GetAnalysisPointRsp, error) {
	rpcClient, err := net.RpcClient("tcp", proxyAddr, time.Second*5)
	if nil != err {
		return nil, err
	}

	req := GetAnalysisPointReq{
		Addr: serverAddr,
		Secs: secs,
	}

	rsp := &GetAnalysisPointRsp{}

	err = rpcClient.Call("Manager.GetAnalysisPoint", req, rsp)

	return rsp, err
}
