package utiltools

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	//"github.com/jordan-wright/email"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

const (
	MIN_CPU_RATE = 20.0
	MIN_MEM_RATE = 40.0
	MAX_CPU_RATE = 90.0
	MAX_MEM_RATE = 90.0
	ARRAY_SIZE   = 10
)

var (
	pprof_mutex      sync.Mutex
	pprof_is_running bool
	cpuArr           [ARRAY_SIZE]float64
	memArr           [ARRAY_SIZE]float64
	// avgCpu float64
	// avgMem float64
)

func CpuAndMemListen() {
	var (
		preUnix     int64
		curUnix     int64
		preCpuTimes float64
		indexCpu    int
		indexMem    int
	)
	preUnix = time.Now().Unix()
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		log.Println(err.Error())
	}
	for {
		flag := false
		time.Sleep(60 * time.Second)
		curUnix = time.Now().Unix()
		diffSecond := curUnix - preUnix
		preUnix = curUnix
		cpuTimes, err := p.Times()
		if err == nil {
			curCpuTimes := cpuTimes.Total()
			cpuPercent := (curCpuTimes - preCpuTimes) * 100.0 / float64(diffSecond)
			// LogInfo(fmt.Sprintf("process preCpuTimes:%v, curCpuTimes: %v, diffSecond:%v, cpuPercent:%f%%\n", preCpuTimes, curCpuTimes, diffSecond, cpuPercent))
			preIndex := (indexCpu + ARRAY_SIZE - 1) % ARRAY_SIZE
			if !flag && (cpuPercent > MAX_CPU_RATE ||
				(cpuPercent > MIN_CPU_RATE && cpuArr[preIndex] > 0 && cpuPercent > cpuArr[preIndex]*2)) {
				// 符合条件, 抓取pprof文件分析
				StartPProf()
				flag = true
			}
			preCpuTimes = curCpuTimes
			cpuArr[indexCpu] = cpuPercent
			indexCpu = (indexCpu + 1) % ARRAY_SIZE
		} else {
			log.Println(err.Error())
		}

		vMem, errV := mem.VirtualMemory()
		if errV == nil {
			memPercent := vMem.UsedPercent
			// LogInfo(fmt.Sprintf("Total: %v, Free:%v, UsedPercent:%f%%\n", vMem.Total, vMem.Free, vMem.UsedPercent))
			preIndex := (indexMem + ARRAY_SIZE - 1) % ARRAY_SIZE
			if !flag && (memPercent > MAX_MEM_RATE ||
				(memPercent > MIN_MEM_RATE && memArr[preIndex] > 0 && memPercent > memArr[preIndex]*2)) {
				// 符合条件, 抓取pprof文件分析
				StartPProf()
				flag = true
			}
			memArr[preIndex] = memPercent
			indexMem = (indexMem + 1) % ARRAY_SIZE
		} else {
			log.Println(errV.Error())
		}
	}
}

func StartPProf() {
	pprof_mutex.Lock()
	if !pprof_is_running {
		pprof_is_running = true
		go collectProfile()
	}
	pprof_mutex.Unlock()
}

func collectProfile() {
	//deployEnv := os.Getenv("DEPLOY_ENV")
	hostname := os.Getenv("HOSTNAME")
	suffix := hostname + "_" + time.Now().Format("20060102_150405") + ".prof"
	memFilename := "/tmp/mem_" + suffix
	filename := "/tmp/cpu_" + suffix
	var (
		//mail     *email.Email
		err error
		f   *os.File
		fm  *os.File
		//needSend bool
	)

	// 采集内存使用情况
	fm, err = os.Create(memFilename)
	if err != nil {
		log.Println(fmt.Sprintf("create pprof file error: %s", err.Error()))
		goto PPROF_UNLOCK
	}
	pprof.WriteHeapProfile(fm)
	time.Sleep(1 * time.Second)

	// 采集CPU使用情况
	f, err = os.Create(filename)
	if err != nil {
		log.Println(fmt.Sprintf("create pprof file error: %s", err.Error()))
		goto PPROF_UNLOCK
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Println(fmt.Sprintf("pprof StartCPUProfile error: %s", err.Error()))
		goto PPROF_UNLOCK
	}
	time.Sleep(60 * time.Second)
	pprof.StopCPUProfile()

	// 发送邮件
	/*needSend = global.GetConfigValAsBool(global.EMAIL_ON)
	if needSend {
		mail = email.NewEmail()
		mail.From = global.GetConfigValAsStr(global.EMAIL_FROM)
		mail.To, _ = global.GetConfigValAsStrings(global.PPROF_MAIL_TO)
		mail.Cc, _ = global.GetConfigValAsStrings(global.PPROF_MAIL_CC)
		mail.Subject = "[" + global.GetConfigValAsStr(global.CLUSTER_ID) + "][" + deployEnv + "]" + hostname + "-应用pprof监控"
		mail.Text = []byte("系统检测到疑似高并发场景，自动采集了应用近60秒的pprof监控信息，请下载附件查看")
		_, _ = mail.AttachFile(filename)
		_, _ = mail.AttachFile(memFilename)
		err = mail.SendWithTLS(global.GetConfigValAsStr(global.EMAIL_HOST)+":"+global.GetConfigValAsStr(global.EMAIL_PORT),
			smtp.PlainAuth("",
				global.GetConfigValAsStr(global.EMAIL_USER),
				global.GetConfigValAsStr(global.EMAIL_PASSWORD),
				global.GetConfigValAsStr(global.EMAIL_HOST)),
			&tls.Config{
				ServerName: global.GetConfigValAsStr(global.EMAIL_HOST),
			})
		if err != nil {
			LogError(fmt.Sprintf("pprof mail.Send error: %s", err.Error()))
		}
	}*/

PPROF_UNLOCK:
	pprof_mutex.Lock()
	pprof_is_running = false
	pprof_mutex.Unlock()
}
