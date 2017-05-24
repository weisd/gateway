package model

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/lunny/log"
)

var (
	_ Register = EtcdStoreV3{}

	// LeaseTTL ttl
	LeaseTTL int64 = 5
)

// Registry Registry clientv3
func (e EtcdStoreV3) Registry(proxyInfo *ProxyInfo) error {

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
	return nil, nil
}

func (e EtcdStoreV3) ChangeLogLevel(proxyAddr string, level string) error {
	return nil
}

func (e EtcdStoreV3) AddAnalysisPoint(proxyAddr, serverAddr string, secs int) error {
	return nil
}

func (e EtcdStoreV3) GetAnalysisPoint(proxyAddr, serverAddr string, secs int) (*GetAnalysisPointRsp, error) {
	return nil, nil
}
