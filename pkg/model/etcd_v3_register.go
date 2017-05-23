package model

import (
	"fmt"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

var (
	_ Register = EtcdStoreV3{}

	// TICKER ticket
	TICKER_V3 = time.Second * 3
	// TTL timeout
	TTL_V3 = uint64(5)
)

// Registry Registry clientv3
func (e EtcdStoreV3) Registry(proxyInfo *ProxyInfo) error {
	timer := time.NewTicker(TICKER_V3)

	go func() {
		for {
			<-timer.C
			log.Debug("Registry start")
			e.doRegistry(proxyInfo)
		}
	}()
	return nil
}

func (e EtcdStore) doRegistry(proxyInfo *ProxyInfo) {
	proxyInfo.Conf.Addr = util.ConvertIP(proxyInfo.Conf.Addr)
	proxyInfo.Conf.MgrAddr = util.ConvertIP(proxyInfo.Conf.MgrAddr)

	key := fmt.Sprintf("%s/%s", e.proxiesDir, proxyInfo.Conf.Addr)
	_, err := e.cli.Set(key, proxyInfo.Marshal(), TTL_V3)

	if err != nil {
		log.ErrorError(err, "Registry fail.")
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
