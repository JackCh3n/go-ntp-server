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
	
	// 解析客户端发送时间 (T1)
	t1Sec := binary.BigEndian.Uint32(req[40:44])
	t1Frac := binary.BigEndian.Uint32(req[44:48])
	
	// 转换为时间对象
	t1 := ntpToTime(t1Sec, t1Frac)
	
	// 获取当前时间作为接收时间 (T2)
	t2 := time.Now().UTC()
	
	// 关键修复：确保T2 >= T1 + 最小处理时间
	// 如果t2早于t1，说明服务器时间比客户端慢，需要调整
	if t2.Before(t1) {
		// 如果服务器时间早于客户端时间，使用客户端时间+1ms
		t2 = t1.Add(1 * time.Millisecond)
	} else if t2.Sub(t1) < 1*time.Millisecond {
		// 如果时间差太小，确保至少有1ms的处理延迟
		t2 = t1.Add(1 * time.Millisecond)
	}
	
	// 构建响应
	resp := buildNTPResponse(req, t1, t2)
	
	// 发送响应，记录实际发送时间
	sendStart := time.Now().UTC()
	_, err := conn.WriteToUDP(resp, addr)
	sendEnd := time.Now().UTC()
	
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
	}
	
	// 记录详细的时间戳信息
	go logRequestDetails(addr, t1, t2, requestArrivalTime, sendStart, sendEnd)
}

func buildNTPResponse(req []byte, t1, t2 time.Time) []byte {
	resp := make([]byte, 48)
	
	// 修复版本号问题：设置正确的NTP版本4
	// 第0字节：LeapIndicator(2 bits) + VersionNumber(3 bits) + Mode(3 bits)
	// 设置LI=0, VN=4, Mode=4
	resp[0] = 0x24 // 00100100: LI=0, VN=4, Mode=4
	
	// Stratum: 2 (二级服务器)
	resp[1] = 2
	
	// Poll: 6 (64秒轮询间隔)
	resp[2] = 6
	
	// Precision: -20 (约微秒级精度)
	resp[3] = 0xEC
	
	// 根延迟设为0
	binary.BigEndian.PutUint32(resp[4:8], 0)
	
	// 根离散设为0
	binary.BigEndian.PutUint32(resp[8:12], 0)
	
	// 参考ID设为"LOCL" (本地时钟)
	copy(resp[12:16], []byte{'L', 'O', 'C', 'L'})
	
	// 参考时间戳：使用当前时间
	refTime := time.Now().UTC()
	setNTPTime(resp[16:24], refTime)
	
	// 原始时间戳 (T1) - 从请求复制
	copy(resp[24:32], req[40:48])
	
	// 接收时间戳 (T2) - 必须晚于T1
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3) - 必须晚于T2
	t3 := t2.Add(1 * time.Millisecond)
	setNTPTime(resp[40:48], t3)
	
	return resp
}

// 将NTP时间转换为Go时间
func ntpToTime(sec uint32, frac uint32) time.Time {
	// 计算从1900年到1970年的秒数
	secondsSince1970 := int64(sec) - int64(ntpEpochOffset)
	
	// 将分数部分转换为纳秒
	nanoseconds := int64(float64(frac) * 1e9 / float64(1<<32))
	
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

// 设置NTP时间戳
func setNTPTime(b []byte, t time.Time) {
	// 计算从1970年到1900年的秒数
	sec := uint32(t.Unix() + ntpEpochOffset)
	
	// 将纳秒转换为NTP分数部分
	frac := uint32(float64(t.Nanosecond()) * float64(1<<32) / 1e9)
	
	binary.BigEndian.PutUint32(b[0:4], sec)
	binary.BigEndian.PutUint32(b[4:8], frac)
}

func logRequestDetails(addr *net.UDPAddr, t1, t2, requestArrival, sendStart, sendEnd time.Time) {
	ip := addr.IP.String()
	names, _ := net.LookupAddr(ip)
	hostname := "-"
	if len(names) > 0 {
		hostname = names[0]
	}
	
	logger.Printf("=== NTP请求详情 ===")
	logger.Printf("客户端: %s (%s)", ip, hostname)
	logger.Printf("T1 (Originate): %s", t1.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("请求到达时间: %s", requestArrival.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("T2 (Receive):   %s", t2.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("发送开始时间:  %s", sendStart.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("发送结束时间:  %s", sendEnd.Format("2006-01-02 15:04:05.000000000"))
	logger.Printf("Delta(T2-T1):  %v", t2.Sub(t1))
	logger.Printf("处理延迟:      %v", sendStart.Sub(requestArrival))
	logger.Printf("发送延迟:      %v", sendEnd.Sub(sendStart))
	logger.Printf("===================\n")
}
