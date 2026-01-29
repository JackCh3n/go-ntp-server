package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var (
	Version = "1.0.0"
	Commit  = "none"
	Date    = "unknown"
)

const (
	ntpEpochOffset = 2208988800 // 1900-1970秒数差
	port           = ":123"
	logFileName    = "ntp_access.log"
	maxBufferSize  = 1024
)

var logger *log.Logger
var requestCounter uint64

func initLogger() {
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	logger = log.New(f, "", log.LstdFlags)
	logger.Printf("NTP Server v%s started at %s", Version, time.Now().Format("2006-01-02 15:04:05"))
}

func main() {
	initLogger()
	
	// 运行自检
	testNTPTimeConversion()
	
	log.Printf("Starting NTP Server v%s (commit: %s, built: %s)", Version, Commit, Date)
	log.Printf("Server will respond as Stratum 1 (Primary Reference) with GPS clock source")

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer conn.Close()
	
	// 设置更大的缓冲区
	conn.SetReadBuffer(maxBufferSize)
	conn.SetWriteBuffer(maxBufferSize)

	log.Printf("NTP server listening on %s", port)
	log.Printf("Log file: %s", logFileName)

	for {
		buf := make([]byte, 1024)
		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("error reading: %v", err)
			continue
		}
		if n < 48 {
			log.Printf("received short packet from %v (size: %d)", clientAddr, n)
			continue
		}
		go handleNTPRequest(conn, clientAddr, buf[:n])
	}
}

func handleNTPRequest(conn *net.UDPConn, addr *net.UDPAddr, req []byte) {
	// 生成请求ID用于跟踪
	reqID := atomic.AddUint64(&requestCounter, 1)
	
	// 记录请求到达时间 (T2)
	requestArrivalTime := time.Now()
	t2 := requestArrivalTime.UTC()
	
	// 解析请求中的传输时间戳 (T1)
	transmitSec := binary.BigEndian.Uint32(req[40:44])
	transmitFrac := binary.BigEndian.Uint32(req[44:48])
	
	// 客户端模式检测
	mode := req[0] & 0x07
	isClientMode := (mode == 3 || mode == 1) // Mode 3 = Client, Mode 1 = Symmetric Active
	
	var t1 time.Time
	if transmitSec == 0 && transmitFrac == 0 {
		// 客户端发送了零时间戳（某些客户端实现）
		if isClientMode {
			// 对于真正的客户端，设置T1为当前时间（这是一种合理的假设）
			t1 = t2.Add(-5 * time.Millisecond) // 假设5ms网络延迟
			logger.Printf("[Req-%d] Client %s sent zero transmit timestamp, using estimated T1 (assumed 5ms delay)", reqID, addr.IP)
		} else {
			// 非客户端模式，使用当前时间
			t1 = time.Now().UTC()
		}
	} else {
		// 正常情况：从请求中解析T1
		t1 = ntpToTime(transmitSec, transmitFrac)
		
		// 验证T1的合理性（不应该在未来）
		if t1.After(t2.Add(2 * time.Second)) {
			logger.Printf("[Req-%d] WARN: Client %s sent future timestamp T1=%s, adjusting to current time", reqID, addr.IP, t1.Format("15:04:05.000"))
			t1 = t2.Add(-1 * time.Millisecond)
		}
	}
	
	// 记录请求解析完成时间
	processTime := time.Now().UTC()
	
	// 构建响应
	resp := buildNTPResponse(req, t1, t2, processTime, reqID)
	
	// 记录发送开始时间
	sendStart := time.Now().UTC()
	
	// 发送响应
	_, err := conn.WriteToUDP(resp, addr)
	sendEnd := time.Now().UTC()
	
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
		return
	}
	
	// 记录详细的时间戳信息
	go logRequestDetails(reqID, addr, t1, t2, requestArrivalTime, processTime, sendStart, sendEnd, req[0])
}

func buildNTPResponse(req []byte, t1, t2, responseTime time.Time, reqID uint64) []byte {
	resp := make([]byte, 48)
	
	// 保存请求头信息
	liVnMode := req[0]
	version := (liVnMode >> 3) & 0x07
	
	// 设置NTP响应头
	// LI=0 (无警告), VN=请求的版本, Mode=4 (服务器模式)
	resp[0] = (liVnMode & 0xC0) | ((version & 0x07) << 3) | 0x04
	
	// Stratum: 1 (一级服务器，表示与GPS等原子钟同步)
	resp[1] = 1
	
	// Poll: 使用请求的轮询间隔或默认值6 (64秒)
	poll := req[2]
	if poll < 4 || poll > 17 {
		poll = 6 // 默认值
	}
	resp[2] = poll
	
	// Precision: -20 (约1微秒精度) - 对应-6 (2^-20 ≈ 1微秒)
	resp[3] = 0xEC // -20的二进制补码表示
	
	// 根延迟: 设置为0x0001 (0.015625 ms)，合理的根延迟
	binary.BigEndian.PutUint32(resp[4:8], 0x00010000)
	
	// 根分散: 设置为0x0001 (0.015625 ms)，合理的根分散
	binary.BigEndian.PutUint32(resp[8:12], 0x00010000)
	
	// 参考ID: 使用"GPS\0" (GPS时钟源)
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 参考时间戳: 使用服务器启动时间（或当前时间减去1秒）
	// 在实际实现中，这应该是最后一次同步到参考源的时间
	refTime := time.Now().UTC().Add(-1 * time.Second)
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳 (T1) - 必须与请求中的传输时间戳一致
	setNTPTime(resp[24:32], t1)
	
	// 接收时间戳 (T2) - 服务器接收时间
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3) - 服务器发送时间
	setNTPTime(resp[40:48], responseTime)
	
	return resp
}

// 将NTP时间转换为Go时间
func ntpToTime(sec uint32, frac uint32) time.Time {
	if sec == 0 && frac == 0 {
		return time.Time{}
	}
	
	// 计算从1970年1月1日开始的秒数
	secondsSince1970 := int64(sec) - ntpEpochOffset
	
	// 将分数部分转换为纳秒
	nanoseconds := int64(float64(frac) * 1e9 / float64(1<<32))
	
	// 处理可能的溢出
	if secondsSince1970 < 0 {
		return time.Time{}
	}
	
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

// 设置NTP时间戳
func setNTPTime(b []byte, t time.Time) {
	if t.IsZero() {
		binary.BigEndian.PutUint32(b[0:4], 0)
		binary.BigEndian.PutUint32(b[4:8], 0)
		return
	}
	
	// 计算从1900年1月1日开始的秒数
	secondsSince1900 := uint64(t.Unix()) + ntpEpochOffset
	
	// 将纳秒转换为NTP分数部分
	// 分数 = (纳秒 * 2^32) / 1e9
	frac := uint64(float64(t.Nanosecond()) * float64(1<<32) / 1e9)
	
	// 写入秒数部分
	binary.BigEndian.PutUint32(b[0:4], uint32(secondsSince1900))
	
	// 写入分数部分
	binary.BigEndian.PutUint32(b[4:8], uint32(frac))
}

func logRequestDetails(reqID uint64, addr *net.UDPAddr, t1, t2, requestArrival, processTime, sendStart, sendEnd time.Time, clientMode byte) {
	ip := addr.IP.String()
	
	// 尝试解析客户端信息
	mode := clientMode & 0x07
	version := (clientMode >> 3) & 0x07
	
	var modeStr string
	switch mode {
	case 1:
		modeStr = "Symmetric Active"
	case 2:
		modeStr = "Symmetric Passive"
	case 3:
		modeStr = "Client"
	case 4:
		modeStr = "Server"
	case 5:
		modeStr = "Broadcast"
	case 6:
		modeStr = "Control"
	case 7:
		modeStr = "Private"
	default:
		modeStr = "Reserved"
	}
	
	logger.Printf("=== NTP请求详情 [Req-%d] ===", reqID)
	logger.Printf("客户端: %s:%d", ip, addr.Port)
	logger.Printf("NTP版本: %d, 模式: %s", version, modeStr)
	
	if !t1.IsZero() {
		logger.Printf("T1 (客户端发送):  %s", t1.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("T2 (服务器接收):  %s", t2.Format("2006-01-02 15:04:05.000000000"))
		
		// 计算延迟
		t1ToT2 := t2.Sub(t1)
		processingDelay := processTime.Sub(requestArrival)
		networkSendDelay := sendEnd.Sub(sendStart)
		totalDelay := sendEnd.Sub(requestArrival)
		
		logger.Printf("T1→T2延迟:       %v", t1ToT2)
		logger.Printf("处理延迟:         %v", processingDelay)
		logger.Printf("网络发送延迟:     %v", networkSendDelay)
		logger.Printf("总处理时间:       %v", totalDelay)
		
		// 计算预期的往返延迟（供客户端使用）
		// 往返延迟 = (T4-T1) - (T3-T2)
		// 时钟偏移 = ((T2-T1) + (T3-T4)) / 2
		logger.Printf("预期的往返延迟:  %v (估计)", t1ToT2+networkSendDelay)
	} else {
		logger.Printf("T1: (未设置或为0)")
		logger.Printf("T2 (服务器接收):  %s", t2.Format("2006-01-02 15:04:05.000000000"))
	}
	
	logger.Printf("===================\n")
}

// 测试NTP时间转换
func testNTPTimeConversion() {
	log.Println("测试NTP时间转换...")
	
	// 测试1: 当前时间
	now := time.Now().UTC()
	var buf [8]byte
	setNTPTime(buf[:], now)
	
	sec := binary.BigEndian.Uint32(buf[0:4])
	frac := binary.BigEndian.Uint32(buf[4:8])
	converted := ntpToTime(sec, frac)
	
	diff := converted.Sub(now)
	if diff.Abs() > time.Microsecond {
		log.Printf("警告: 时间转换误差: %v (允许范围: 1微秒)", diff)
	} else {
		log.Println("时间转换测试通过")
	}
	
	// 测试2: 零时间
	zeroTime := time.Time{}
	setNTPTime(buf[:], zeroTime)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	if sec != 0 || frac != 0 {
		log.Printf("错误: 零时间转换失败: sec=%d, frac=%d", sec, frac)
	} else {
		log.Println("零时间转换测试通过")
	}
	
	// 测试3: 特定时间
	testTime := time.Date(2026, 1, 29, 14, 9, 0, 0, time.UTC)
	setNTPTime(buf[:], testTime)
	sec = binary.BigEndian.Uint32(buf[0:4])
	frac = binary.BigEndian.Uint32(buf[4:8])
	converted = ntpToTime(sec, frac)
	
	if !converted.Equal(testTime) {
		log.Printf("错误: 特定时间转换失败: 期望 %v, 得到 %v", testTime, converted)
	} else {
		log.Println("特定时间转换测试通过")
	}
}
