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
	// 解析客户端发送时间 (T1)
	t1Sec := binary.BigEndian.Uint32(req[40:44])
	t1Frac := binary.BigEndian.Uint32(req[44:48])
	t1 := ntpToTime(t1Sec, t1Frac)
	
	// 获取高精度接收时间 (T2)
	t2 := time.Now().UTC()
	
	// 模拟处理延迟（1-5ms）
	processingDelay := time.Duration(1+time.Now().Nanosecond()%5) * time.Millisecond
	time.Sleep(processingDelay)
	
	// 获取高精度传输时间 (T3)
	t3 := time.Now().UTC()
	
	// 构建响应包
	resp := buildResponse(t1, t2, t3)
	
	_, err := conn.WriteToUDP(resp, addr)
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
	}

	// 记录详细时间戳信息
	go logTimestamps(addr, t1, t2, t3)
}

func buildResponse(t1, t2, t3 time.Time) []byte {
	resp := make([]byte, 48)
	
	// NTP头部: LI=0, VN=4, Mode=4 (服务器模式)
	resp[0] = 0x24 // 00100100
	
	// Stratum: 2 (二级服务器)
	resp[1] = 2
	
	// Poll: 10 (1024秒轮询间隔)
	resp[2] = 0x0A
	
	// Precision: -20 (约微秒级精度)
	resp[3] = 0xEC
	
	// 根延迟和根离散设为合理值
	binary.BigEndian.PutUint32(resp[4:8], 0x00000E18) // 1ms延迟
	binary.BigEndian.PutUint32(resp[8:12], 0x00002710) // 10ms离散
	
	// 参考ID设为"GPS\0"（模拟GPS时间源）
	copy(resp[12:16], []byte{'G', 'P', 'S', 0})
	
	// 参考时间戳（模拟GPS时间）
	setNTPTime(resp[16:24], time.Now().UTC().Add(-24*time.Hour))
	
	// 原始时间戳 (T1)
	setNTPTime(resp[24:32], t1)
	
	// 接收时间戳 (T2)
	setNTPTime(resp[32:40], t2)
	
	// 传输时间戳 (T3)
	setNTPTime(resp[40:48], t3)

	return resp
}

// 将NTP时间转换为Go时间
func ntpToTime(sec uint32, frac uint32) time.Time {
	secondsSince1970 := int64(sec) - int64(ntpEpochOffset)
	nanoseconds := int64(frac) * 1e9 / (1 << 32)
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

// 设置NTP时间戳（高精度版本）
func setNTPTime(b []byte, t time.Time) {
	secs := uint64(t.Unix() + ntpEpochOffset)
	nanosecs := uint64(t.Nanosecond())
	
	// 计算秒和小数部分
	frac := (nanosecs << 32) / 1e9
	
	binary.BigEndian.PutUint32(b[0:4], uint32(secs))
	binary.BigEndian.PutUint32(b[4:8], uint32(frac))
}

func logTimestamps(addr *net.UDPAddr, t1, t2, t3 time.Time) {
	ip := addr.IP.String()
	names, err := net.LookupAddr(ip)
	hostname := "-"
	if err == nil && len(names) > 0 {
		hostname = names[0]
	}
	
	logger.Printf("Request from %s (%s)", ip, hostname)
	logger.Printf("T1 (Originate): %s", t1.Format(time.RFC3339Nano))
	logger.Printf("T2 (Receive):   %s", t2.Format(time.RFC3339Nano))
	logger.Printf("T3 (Transmit):  %s", t3.Format(time.RFC3339Nano))
	logger.Printf("Delta(T2-T1):  %dns", t2.Sub(t1).Nanoseconds())
	logger.Printf("Delta(T3-T2):  %dns", t3.Sub(t2).Nanoseconds())
	logger.Printf("----------------------------------------")
}