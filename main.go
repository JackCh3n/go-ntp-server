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
	// 记录请求到达时间
	requestArrivalTime := time.Now().UTC()
	
	// 解析请求中的传输时间戳（T1）
	// Windows NTP客户端在发送请求时，会在Transmit Timestamp字段放置发送时间
	// 对于NTP客户端模式（模式3），Transmit Timestamp位于40-47字节
	transmitSec := binary.BigEndian.Uint32(req[40:44])
	transmitFrac := binary.BigEndian.Uint32(req[44:48])
	
	// 将客户端传输时间戳转换为时间
	t1 := ntpToTime(transmitSec, transmitFrac)
	
	// 处理T1为0的情况
	var t2 time.Time
	if transmitSec == 0 && transmitFrac == 0 {
		// Windows客户端有时会发送0时间戳，此时需要特殊处理
		// 使用当前时间作为T2，并设置合理的T1为当前时间-1ms
		t2 = requestArrivalTime
		// 设置T1为请求到达时间减去网络延迟估计（1ms）
		t1 = requestArrivalTime.Add(-1 * time.Millisecond)
		logger.Printf("INFO: Client %s sent zero transmit timestamp, using estimated T1", addr.IP)
	} else {
		// 正常情况：使用请求到达时间作为T2
		t2 = requestArrivalTime
		
		// 确保T2 >= T1
		if t2.Before(t1) {
			// 如果服务器时间比客户端慢，调整T2
			t2 = t1.Add(1 * time.Microsecond)
		}
	}
	
	// 记录请求解析完成时间
	processTime := time.Now().UTC()
	
	// 构建响应
	resp := buildNTPResponse(req, t1, t2, processTime)
	
	// 发送响应
	sendStart := time.Now().UTC()
	_, err := conn.WriteToUDP(resp, addr)
	sendEnd := time.Now().UTC()
	
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
	}
	
	// 记录详细的时间戳信息
	go logRequestDetails(addr, t1, t2, requestArrivalTime, processTime, sendStart, sendEnd)
}

func buildNTPResponse(req []byte, t1, t2, responseTime time.Time) []byte {
	resp := make([]byte, 48)
	
	// 设置NTP响应头
	// 字节0: LeapIndicator(2 bits) + VersionNumber(3 bits) + Mode(3 bits)
	// LI=0 (无警告), VN=4 (NTPv4), Mode=4 (服务器)
	resp[0] = 0x24
	
	// Stratum: 1 (一级服务器，表示与GPS等原子钟同步)
	// Windows客户端期望Stratum为1或2
	resp[1] = 1
	
	// Poll: 6 (64秒轮询间隔) - 这是合理的服务器值
	resp[2] = 6
	
	// Precision: -20 (约1微秒精度)
	resp[3] = 236
	
	// 根延迟: 设置为0 (表示与参考时钟在同一台机器上)
	binary.BigEndian.PutUint32(resp[4:8], 0)
	
	// 根分散: 设置为0.000015秒 (15µs)，这是NTP标准的典型值
	// 0.000015秒 = 0x0000.0001 (十六进制表示)
	binary.BigEndian.PutUint32(resp[8:12], 0x00000001)
	
	// 参考ID: 使用"GPS\0" (GPS时钟源)
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 参考时间戳: 当前时间减去1秒，模拟合理的参考时钟
	refTime := time.Now().UTC().Add(-1 * time.Second)
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳 (T1) - 从请求的Transmit Timestamp复制
	// Windows客户端期望在响应中看到它发送的Transmit Timestamp
	copy(resp[24:32], req[40:48])
	
	// 接收时间戳 (T2) - 服务器接收时间
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3) - 服务器发送时间
	setNTPTime(resp[40:48], responseTime)
	
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

func logRequestDetails(addr *net.UDPAddr, t1, t2, requestArrival, processTime, sendStart, sendEnd time.Time) {
	ip := addr.IP.String()
	names, _ := net.LookupAddr(ip)
	hostname := "-"
	if len(names) > 0 {
		hostname = names[0]
	}
	
	logger.Printf("=== NTP请求详情 ===")
	logger.Printf("客户端: %s (%s)", ip, hostname)
	
	if !t1.IsZero() {
		logger.Printf("T1 (Transmit):    %s", t1.Format("2006-01-02 15:04:05.000000000"))
	} else {
		logger.Printf("T1 (Transmit):    (未设置或为0)")
	}
	
	logger.Printf("请求到达时间:     %s", requestArrival.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("T2 (Receive):      %s", t2.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("处理完成时间:     %s", processTime.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("发送开始时间:     %s", sendStart.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("发送结束时间:     %s", sendEnd.Format("2006-01-02 15:04:05.000000000"))
	
	// 计算各种延迟
	if !t1.IsZero() {
		logger.Printf("T1→请求到达延迟: %v", requestArrival.Sub(t1))
		logger.Printf("T1→T2延迟:       %v", t2.Sub(t1))
	}
	logger.Printf("请求处理延迟:     %v", processTime.Sub(requestArrival))
	logger.Printf("响应构建延迟:     %v", sendStart.Sub(processTime))
	logger.Printf("网络发送延迟:     %v", sendEnd.Sub(sendStart))
	logger.Printf("总处理延迟:       %v", sendEnd.Sub(requestArrival))
	
	logger.Printf("===================\n")
}

// 测试NTP时间转换
func testNTPTimeConversion() {
	log.Println("测试NTP时间转换...")
	
	// 测试当前时间
	now := time.Now().UTC()
	var buf [8]byte
	setNTPTime(buf[:], now)
	
	sec := binary.BigEndian.Uint32(buf[0:4])
	frac := binary.BigEndian.Uint32(buf[4:8])
	converted := ntpToTime(sec, frac)
	
	diff := converted.Sub(now)
	if diff.Abs() > time.Microsecond {
		log.Printf("警告: 时间转换误差: %v", diff)
	} else {
		log.Println("时间转换测试通过")
	}
}
