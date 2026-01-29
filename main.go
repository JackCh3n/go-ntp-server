package main

import (
	"encoding/binary"
	"log"
	"net"
	"os"
	"time"
)

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

const (
	ntpEpochOffset = 2208988800 // 1900-1970秒数差
	port           = ":123"
	logFileName    = "ntp_access.log"
)

var logger *log.Logger

func initLogger() {
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	logger = log.New(f, "", log.LstdFlags)
}

func main() {
	initLogger()
	log.Printf("Starting NTP Server v%s (commit: %s, built: %s)", Version, Commit, Date)

	addr, err := net.ResolveUDPAddr("udp", port)
	if err != nil {
		log.Fatalf("failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer conn.Close()

	log.Printf("NTP server listening on %s", port)

	for {
		buf := make([]byte, 48)
		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("error reading: %v", err)
			continue
		}
		if n < 48 {
			log.Printf("received short packet from %v", clientAddr)
			continue
		}
		go handleNTPRequest(conn, clientAddr, buf)
	}
}

func handleNTPRequest(conn *net.UDPConn, addr *net.UDPAddr, req []byte) {
	// 修复1: 首先记录请求到达时间
	requestArrivalTime := time.Now().UTC()
	
	// 修复2: 正确解析NTP请求头，获取第一个有效时间戳
	// NTP报文结构: 前48字节
	// 字节0: LI(2) + VN(3) + Mode(3)
	// 字节1-3: Stratum, Poll, Precision
	// 字节4-39: 各种延迟和分散字段
	// 字节40-47: 参考时间戳
	// 字节24-31: 原始时间戳(T1) - 这才是客户端发送的时间
	// 字节32-39: 接收时间戳(T2) - 服务器接收时间
	// 字节40-47: 传输时间戳(T3) - 服务器发送时间
	
	// 修复3: 从正确的位置解析T1
	// 根据NTP协议，原始时间戳(Originate Timestamp)在第24-31字节
	originateSec := binary.BigEndian.Uint32(req[24:28])
	originateFrac := binary.BigEndian.Uint32(req[28:32])
	
	// 将客户端原始时间戳(T1)转换为时间
	t1 := ntpToTime(originateSec, originateFrac)
	
	// 修复4: 正确处理T1为0的情况（某些客户端可能不设置）
	var t2 time.Time
	if originateSec == 0 && originateFrac == 0 {
		// 如果客户端没有设置原始时间戳，使用当前时间减去估计的处理时间
		t2 = requestArrivalTime.Add(-1 * time.Millisecond)
		logger.Printf("WARN: Client %s sent zero originate timestamp, using estimated T1", addr.IP)
	} else {
		// 使用请求到达时间作为T2（服务器接收时间）
		t2 = requestArrivalTime
		
		// 修复5: 验证时间戳的合理性
		// 如果T1晚于T2超过1分钟，说明客户端时间可能过快
		if t1.After(t2) {
			diff := t1.Sub(t2)
			if diff > 60*time.Second {
				logger.Printf("WARN: Client %s clock appears to be %v ahead of server", addr.IP, diff)
				// 调整T2为T1+1ms，保持时间顺序正确
				t2 = t1.Add(1 * time.Millisecond)
			}
		}
		
		// 确保T2至少比T1晚1微秒，避免时间倒退
		if t2.Sub(t1) <= 0 {
			t2 = t1.Add(1 * time.Microsecond)
		}
	}
	
	// 修复6: 计算传输时间戳(T3) - 在实际发送前计算
	t3 := time.Now().UTC()
	
	// 确保T3 > T2
	if t3.Sub(t2) <= 0 {
		t3 = t2.Add(1 * time.Millisecond)
	}
	
	// 构建响应
	resp := buildNTPResponse(req, t1, t2, t3, requestArrivalTime)
	
	// 记录发送开始时间
	sendStart := time.Now().UTC()
	
	// 发送响应
	_, err := conn.WriteToUDP(resp, addr)
	
	// 记录发送结束时间
	sendEnd := time.Now().UTC()
	
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
	}
	
	// 记录详细的时间戳信息
	go logRequestDetails(addr, t1, t2, requestArrivalTime, sendStart, sendEnd, t3)
}

func buildNTPResponse(req []byte, t1, t2, t3, requestArrival time.Time) []byte {
	resp := make([]byte, 48)
	
	// 设置NTP版本4，服务器模式
	// LI=0 (无警告), VN=4 (NTPv4), Mode=4 (服务器)
	resp[0] = 0x24
	
	// Stratum: 2 (二级服务器，从GPS同步)
	resp[1] = 2
	
	// Poll: 6 (64秒)
	resp[2] = 6
	
	// Precision: -20 (约1微秒)
	resp[3] = 236
	
	// 根延迟: 0.001秒
	binary.BigEndian.PutUint32(resp[4:8], 0x00010000)
	
	// 根分散: 0.001秒
	binary.BigEndian.PutUint32(resp[8:12], 0x00010000)
	
	// 参考ID: "LOCL" 表示本地时钟
	copy(resp[12:16], []byte{'L', 'O', 'C', 'L'})
	
	// 参考时间戳: 服务器最后同步的时间
	refTime := time.Now().UTC().Add(-5 * time.Second)
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳(T1) - 从请求复制
	copy(resp[24:32], req[24:32])
	
	// 接收时间戳(T2)
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳(T3) - 使用传入的t3
	setNTPTime(resp[40:48], t3)
	
	return resp
}

// 将NTP时间转换为Go时间
func ntpToTime(sec uint32, frac uint32) time.Time {
	// 检查是否为0时间戳
	if sec == 0 && frac == 0 {
		return time.Time{}
	}
	
	// 计算从1900年到1970年的秒数
	secondsSince1970 := int64(sec) - int64(ntpEpochOffset)
	
	// 将分数部分转换为纳秒
	// 注意：NTP时间戳的分数部分是1/2^32秒
	nanoseconds := int64(float64(frac) * 1e9 / float64(1<<32))
	
	// 处理可能的负数（虽然NTP时间不应该为负）
	if secondsSince1970 < 0 {
		return time.Unix(0, 0).UTC()
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

func logRequestDetails(addr *net.UDPAddr, t1, t2, requestArrival, sendStart, sendEnd, t3 time.Time) {
	ip := addr.IP.String()
	names, _ := net.LookupAddr(ip)
	hostname := "-"
	if len(names) > 0 {
		hostname = names[0]
	}
	
	// 计算各种时间差
	t1ToRequestArrival := requestArrival.Sub(t1)
	t1ToT2 := t2.Sub(t1)
	processingDelay := sendStart.Sub(requestArrival)
	sendDelay := sendEnd.Sub(sendStart)
	t2ToT3 := t3.Sub(t2)
	totalRoundTrip := sendEnd.Sub(requestArrival)
	
	// 修复7: 更详细的日志记录
	logger.Printf("=== NTP请求详情 ===")
	logger.Printf("客户端: %s (%s)", ip, hostname)
	
	// 只有T1有效时才记录
	if !t1.IsZero() {
		logger.Printf("T1 (Originate):   %s", t1.Format("2006-01-02 15:04:05.000000000"))
		logger.Printf("请求到达时间:     %s (T1到达延迟: %v)", 
			requestArrival.Format("2006-01-02 15:04:05.000000000"), t1ToRequestArrival)
	} else {
		logger.Printf("T1 (Originate):   (未设置或为0)")
		logger.Printf("请求到达时间:     %s", 
			requestArrival.Format("2006-01-02 15:04:05.000000000"))
	}
	
	logger.Printf("T2 (Receive):      %s (T2-T1: %v)", 
		t2.Format("2006-01-02 15:04:05.000000000"), t1ToT2)
	logger.Printf("T3 (Transmit):     %s (T3-T2: %v)", 
		t3.Format("2006-01-02 15:04:05.000000000"), t2ToT3)
	logger.Printf("发送开始时间:      %s", sendStart.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("发送结束时间:      %s", sendEnd.Format("2006-01-02 15:04:05.000000000"))
	
	// 时间统计
	logger.Printf("时间统计:")
	logger.Printf("  T1→请求到达:    %v", t1ToRequestArrival)
	logger.Printf("  T1→T2:         %v", t1ToT2)
	logger.Printf("  T2→T3:         %v", t2ToT3)
	logger.Printf("  处理延迟:       %v", processingDelay)
	logger.Printf("  发送延迟:       %v", sendDelay)
	logger.Printf("  总往返时间:     %v", totalRoundTrip)
	
	// 验证时间顺序
	if !t1.IsZero() && t1.After(requestArrival) {
		logger.Printf("警告: T1晚于请求到达时间! 客户端时钟可能过快或解析错误")
	}
	
	if t2.After(t3) {
		logger.Printf("错误: T2晚于T3! 时间顺序错误")
	}
	
	if sendStart.After(sendEnd) {
		logger.Printf("错误: 发送开始晚于发送结束!")
	}
	
	logger.Printf("===================\n")
}

// 测试函数
func testNTPConversion() {
	// 测试ntpToTime和setNTPTime函数的正确性
	testTime := time.Date(2026, 1, 29, 12, 0, 0, 0, time.UTC)
	
	// 将时间转换为NTP格式
	var ntpBuf [8]byte
	setNTPTime(ntpBuf[:], testTime)
	
	// 再转换回来
	sec := binary.BigEndian.Uint32(ntpBuf[0:4])
	frac := binary.BigEndian.Uint32(ntpBuf[4:8])
	convertedTime := ntpToTime(sec, frac)
	
	// 比较时间（允许1微秒的误差）
	diff := convertedTime.Sub(testTime)
	if diff.Abs() > time.Microsecond {
		log.Printf("WARN: Time conversion error: diff = %v", diff)
	} else {
		log.Printf("INFO: Time conversion test passed")
	}
}
