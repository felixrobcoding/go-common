package sentinel

import (
	"cp.wjaiot.net/cloud-server/smarlife/SaaSBP/go-common/sentinel/etcdv3"
	"fmt"
	sentinel "github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/alibaba/sentinel-golang/ext/datasource"
	"github.com/alibaba/sentinel-golang/logging"
	"go.etcd.io/etcd/client/v3"
	"time"
)

type Config struct {
	EtcdAddr             string
	FlowToggle           bool // 限流
	CircuitBreakerToggle bool // 熔断降级
	HotSpotParamToggle   bool // 热点参数
	IsolationToggle      bool // 并发隔离
	SystemRules          bool // 系统规则
}

// StartSentinel 启动
func StartSentinel(conf *Config) (err error) {
	if conf == nil || conf.EtcdAddr == "" {
		return fmt.Errorf("illegal params")
	}
	// 启动限流
	obj := newSentinel(conf.EtcdAddr)
	if conf.FlowToggle {
		err = obj.StartFlowRulesDatasource()
		if err != nil {
			return
		}
	}

	// 启动熔断降级
	if conf.CircuitBreakerToggle {
		err = obj.StartCircuitBreakerRulesDatasource()
		if err != nil {
			return
		}
	}

	// 启动热点参数
	if conf.HotSpotParamToggle {
		err = obj.StartHotSpotParamRulesDatasource()
		if err != nil {
			return
		}
	}

	// 启动并发隔离控制
	if conf.IsolationToggle {
		err = obj.StartIsolationRulesDatasource()
		if err != nil {
			return
		}
	}

	// 启动系统自适应规则
	if conf.SystemRules {
		err = obj.StartSystemRulesDatasource()
		if err != nil {
			return
		}
	}
	return
}

// Sentinel 限流、熔断、降级组件
type Sentinel struct {
	etcdAddr string
}

func newSentinel(etcdAddr string) *Sentinel {
	// We should initialize Sentinel first.
	conf := config.NewDefaultConfig()
	// for testing, logging output to console
	conf.Sentinel.Log.Logger = logging.NewConsoleLogger()
	err := sentinel.InitWithConfig(conf)
	if err != nil {
		fmt.Println(err)
	}

	return &Sentinel{
		etcdAddr: etcdAddr,
	}
}

// StartFlowRulesDatasource 限流
func (s *Sentinel) StartFlowRulesDatasource() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	h := datasource.NewFlowRulesHandler(datasource.FlowRuleJsonArrayParser)
	ds, err := etcdv3.NewDataSource(cli, "/sentinel/flow", h)
	if err != nil {
		return err
	}
	err = ds.Initialize()
	return err
}

// StartCircuitBreakerRulesDatasource 熔断降级
func (s *Sentinel) StartCircuitBreakerRulesDatasource() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	h := datasource.NewCircuitBreakerRulesHandler(datasource.CircuitBreakerRuleJsonArrayParser)
	ds, err := etcdv3.NewDataSource(cli, "/sentinel/breaker", h)
	if err != nil {
		return err
	}
	err = ds.Initialize()
	return err
}

// StartHotSpotParamRulesDatasource 热点参数流控
func (s *Sentinel) StartHotSpotParamRulesDatasource() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	h := datasource.NewHotSpotParamRulesHandler(datasource.HotSpotParamRuleJsonArrayParser)
	ds, err := etcdv3.NewDataSource(cli, "/sentinel/hotspot", h)
	if err != nil {
		return err
	}
	err = ds.Initialize()
	return err
}

// StartIsolationRulesDatasource 并发隔离控制
func (s *Sentinel) StartIsolationRulesDatasource() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	h := datasource.NewIsolationRulesHandler(datasource.IsolationRuleJsonArrayParser)
	ds, err := etcdv3.NewDataSource(cli, "/sentinel/isolation", h)
	if err != nil {
		return err
	}
	err = ds.Initialize()
	return err
}

// StartSystemRulesDatasource 系统自适应规则
func (s *Sentinel) StartSystemRulesDatasource() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}

	h := datasource.NewSystemRulesHandler(datasource.SystemRuleJsonArrayParser)
	ds, err := etcdv3.NewDataSource(cli, "/sentinel/system", h)
	if err != nil {
		return err
	}
	err = ds.Initialize()
	return err
}
