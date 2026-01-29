package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var (
	Version = "1.2.0"
	Commit  = "fixed_time_sync"
	Date    = "2026-01-29"
)

const (
	ntpEpochOffset = 2208988800 // 1900-1970秒数差
	port           = ":123"
	logFileName    = "ntp_access.log"
	maxBufferSize  = 1024
)

var logger *log.Logger
var requestCounter uint64
var serverStartTime time.Time
var fakeCurrentTime time.Time // 模拟的当前时间，用于测试

func initLogger() {
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	logger = log.New(f, "", log.LstdFlags)
	
	// 设置服务器启动时间为当前实际时间
	serverStartTime = time.Now()
	
	// 设置一个错误的模拟当前时间（2016年），但我们会根据客户端请求调整
	fakeCurrentTime = time.Date(2016, 5, 29, 17, 51, 48, 866673800, time.UTC)
	
	logger.Printf("NTP Server v%s started at %s (Actual UTC: %s)", Version,
		fakeCurrentTime.Format("2006-01-02 15:04:05"),
		serverStartTime.UTC().Format("2006-01-02 15:04:05"))
	log.Printf("NTP Server v%s started", Version)
	log.Printf("Simulated server time: %s (for testing)", fakeCurrentTime.Format("2006-01-02 15:04:05"))
	log.Printf("Actual server time: %s", serverStartTime.UTC().Format("2006-01-02 15:04:05"))
	log.Printf("Server will respond as Stratum 1 with GPS clock source")
}

func main() {
	initLogger()
	
	// 运行自检
	testNTPTimeConversion()
	
	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer conn.Close()
	
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
	reqID := atomic.AddUint64(&requestCounter, 1)
	
	// 记录请求到达时间 (T2)
	requestArrivalTime := time.Now()
	t2 := requestArrivalTime.UTC()
	
	// 解析请求中的传输时间戳 (T1)
	transmitSec := binary.BigEndian.Uint32(req[40:44])
	transmitFrac := binary.BigEndian.Uint32(req[44:48])
	
	// 解析客户端模式
	mode := req[0] & 0x07
	version := (req[0] >> 3) & 0x07
	
	var t1 time.Time
	if transmitSec == 0 && transmitFrac == 0 {
		// 如果客户端发送了零时间戳，使用当前时间减去估计的延迟
		estimatedDelay := 5 * time.Millisecond
		t1 = t2.Add(-estimatedDelay)
		logger.Printf("[Req-%d] Client %s sent zero transmit timestamp, using estimated T1", reqID, addr.IP)
	} else {
		// 正常解析客户端时间戳
		t1 = ntpToTime(transmitSec, transmitFrac)
		
		if t1.IsZero() {
			logger.Printf("[Req-%d] Client %s sent invalid timestamp, using current time", reqID, addr.IP)
			t1 = t2.Add(-5 * time.Millisecond)
		}
	}
	
	// 关键修复：计算时间偏移，确保响应时间在Windows允许范围内
	// Windows只允许±54000秒（15小时）的时间调整
	maxAllowedOffset := 54000 * time.Second
	
	// 使用模拟的服务器时间
	serverTime := fakeCurrentTime
	
	// 计算客户端与服务器的时间差
	timeDiff := t1.Sub(serverTime)
	
	// 记录原始时间差
	logger.Printf("[Req-%d] Raw time difference: client %s - server %s = %v", 
		reqID, t1.Format("2006-01-02 15:04:05"), 
		serverTime.Format("2006-01-02 15:04:05"), timeDiff)
	
	// 如果时间差超过Windows允许范围，调整服务器返回的时间
	var adjustedServerTime time.Time
	if timeDiff.Abs() > maxAllowedOffset {
		// 计算调整量，确保在允许范围内
		if timeDiff > 0 {
			// 客户端比服务器快，调整服务器时间向前
			adjustedServerTime = t1.Add(-maxAllowedOffset + 10*time.Second)
		} else {
			// 客户端比服务器慢，调整服务器时间向后
			adjustedServerTime = t1.Add(maxAllowedOffset - 10*time.Second)
		}
		
		logger.Printf("[Req-%d] Time offset too large (|%v| > %v), adjusting server time for response", 
			reqID, timeDiff, maxAllowedOffset)
		logger.Printf("[Req-%d] Original server time: %s", reqID, serverTime.Format("2006-01-02 15:04:05"))
		logger.Printf("[Req-%d] Adjusted server time: %s", reqID, adjustedServerTime.Format("2006-01-02 15:04:05"))
		logger.Printf("[Req-%d] Adjusted offset: %v", reqID, t1.Sub(adjustedServerTime))
	} else {
		// 时间差在允许范围内，使用模拟服务器时间
		adjustedServerTime = serverTime
	}
	
	// 构建响应，使用调整后的时间
	resp := buildNTPResponse(req, t1, t2, adjustedServerTime, reqID)
	
	// 发送响应
	_, err := conn.WriteToUDP(resp, addr)
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
		return
	}
	
	// 记录请求详情
	logRequestDetails(reqID, addr, t1, t2, requestArrivalTime, adjustedServerTime, req[0], timeDiff)
}

func buildNTPResponse(req []byte, t1, t2, responseTime time.Time, reqID uint64) []byte {
	resp := make([]byte, 48)
	
	// 保存请求头信息
	liVnMode := req[0]
	version := (liVnMode >> 3) & 0x07
	
	// 设置NTP响应头
	// LI=0 (无警告), VN=请求的版本, Mode=4 (服务器模式)
	resp[0] = ((version & 0x07) << 3) | 0x04
	
	// Stratum: 1 (一级服务器)
	resp[1] = 1
	
	// Poll: 使用请求的轮询间隔或默认值
	poll := req[2]
	if poll < 4 || poll > 15 {
		poll = 6
	}
	resp[2] = poll
	
	// Precision: -20 (约1微秒精度)
	resp[3] = 0xEC
	
	// 根延迟: 设置为很小的值
	binary.BigEndian.PutUint32(resp[4:8], 0x00000001)
	
	// 根分散: 设置为很小的值
	binary.BigEndian.PutUint32(resp[8:12], 0x00000001)
	
	// 参考ID: 使用"GPS\0" (GPS时钟源)
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 关键修复：使用服务器启动时间作为参考时间
	// 这应该是服务器最后一次同步的时间
	refTime := serverStartTime.UTC()
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳 (T1) - 必须与请求中的传输时间戳完全一致
	// 这是NTP协议的关键：服务器必须原样返回客户端的时间戳
	copy(resp[24:32], req[40:48])
	
	// 接收时间戳 (T2) - 服务器接收时间
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3) - 服务器发送时间（使用调整后的时间）
	setNTPTime(resp[40:48], responseTime)
	
	// 验证响应时间戳的正确性
	respT1Sec := binary.BigEndian.Uint32(resp[24:28])
	respT1Frac := binary.BigEndian.Uint32(resp[28:32])
	reqT1Sec := binary.BigEndian.Uint32(req[40:44])
	reqT1Frac := binary.BigEndian.Uint32(req[44:48])
	
	if respT1Sec != reqT1Sec || respT1Frac != reqT1Frac {
		logger.Printf("[Req-%d] ERROR: OriginateTimestamp mismatch! Response: %08X.%08X, Request: %08X.%08X",
			reqID, respT1Sec, respT1Frac, reqT1Sec, reqT1Frac)
	} else {
		logger.Printf("[Req-%d] OriginateTimestamp correctly matches request", reqID)
	}
	
	return resp
}

func ntpToTime(sec uint32, frac uint32) time.Time {
	if sec == 0 && frac == 0 {
		return time.Time{}
	}
	
	// 计算自1900年以来的秒数
	secondsSince1900 := int64(sec)
	
	// 转换为自1970年以来的秒数
	secondsSince1970 := secondsSince1900 - ntpEpochOffset
	
	// 如果结果小于0，可能是无效时间戳
	if secondsSince1970 < 0 {
		// 对于非常古老的时间戳，可能表示时间在1970年之前
		// 但在实际NTP中，这通常表示错误
		return time.Time{}
	}
	
	// 计算纳秒部分
	nanoseconds := int64(float64(frac) * 1e9 / float64(uint64(1)<<32))
	
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

func setNTPTime(b []byte, t time.Time) {
	if t.IsZero() {
		binary.BigEndian.PutUint32(b[0:4], 0)
		binary.BigEndian.PutUint32(b[4:8], 0)
		return
	}
	
	unixTime := t.Unix()
	secondsSince1900 := uint64(unixTime) + ntpEpochOffset
	
	// 计算小数部分
	frac := uint64(float64(t.Nanosecond()) * float64(uint64(1)<<32) / 1e9)
	
	binary.BigEndian.PutUint32(b[0:4], uint32(secondsSince1900))
	binary.BigEndian.PutUint32(b[4:8], uint32(frac))
}

func logRequestDetails(reqID uint64, addr *net.UDPAddr, t1, t2, requestArrival, responseTime time.Time, clientMode byte, timeDiff time.Duration) {
	ip := addr.IP.String()
	
	mode := clientMode & 0x07
	version := (clientMode >> 3) & 0x07
	
	var modeStr string
	switch mode {
	case 3:
		modeStr = "Client"
	case 4:
		modeStr = "Server"
	default:
		modeStr = fmt.Sprintf("Mode %d", mode)
	}
	
	logger.Printf("=== NTP请求详情 [Req-%d] ===", reqID)
	logger.Printf("客户端: %s:%d", ip, addr.Port)
	logger.Printf("NTP版本: %d, 模式: %s", version, modeStr)
	
	if !t1.IsZero() {
		logger.Printf("T1 (客户端发送时间):  %s", t1.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("T2 (服务器接收时间):  %s", t2.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("T3 (服务器发送时间):  %s", responseTime.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("原始时间差: %v (客户端-服务器)", timeDiff)
		
		// 计算NTP时间偏移
		// NTP offset = ((T2 - T1) + (T3 - T4)) / 2
		// 这里T4是客户端接收时间，我们不知道，但可以估计
		estimatedNetworkDelay := 10 * time.Millisecond
		t4 := t1.Add(estimatedNetworkDelay)
		ntpOffset := ((t2.Sub(t1) + responseTime.Sub(t4)) / 2)
		
		logger.Printf("估计的NTP时间偏移: %v", ntpOffset)
		logger.Printf("     (正值表示客户端比服务器快)")
		
		// 检查是否在Windows允许范围内
		maxAllowed := 54000 * time.Second
		if ntpOffset.Abs() > maxAllowed {
			logger.Printf("警告: 时间偏移超出Windows允许范围 (±%v)", maxAllowed)
			logger.Printf("      Windows时间服务将拒绝此同步")
		} else {
			logger.Printf("良好: 时间偏移在Windows允许范围内 (±%v)", maxAllowed)
			logger.Printf("      Windows时间服务应接受此同步")
		}
	}
	
	logger.Printf("===================\n")
}

func testNTPTimeConversion() {
	log.Println("运行NTP时间转换测试...")
	
	// 测试当前时间转换
	now := time.Now().UTC()
	var buf [8]byte
	setNTPTime(buf[:], now)
	sec := binary.BigEndian.Uint32(buf[0:4])
	frac := binary.BigEndian.Uint32(buf[4:8])
	converted := ntpToTime(sec, frac)
	
	if !converted.Round(time.Millisecond).Equal(now.Round(time.Millisecond)) {
		log.Printf("时间转换测试失败: 期望 %v, 得到 %v", now, converted)
	} else {
		log.Println("时间转换测试通过")
	}
	
	// 测试模拟的服务器时间
	log.Printf("模拟服务器时间: %s", fakeCurrentTime.Format("2006-01-02 15:04:05"))
	log.Println("NTP服务器测试完成")
}
