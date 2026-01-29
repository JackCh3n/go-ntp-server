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
	resp := make([]byte, 48)
	
	// NTP头部: LI=0, VN=4, Mode=4 (服务器模式)
	resp[0] = 0x24 // 00100100
	
	// Stratum: 1 (一级服务器)
	resp[1] = 1
	
	// Poll: 10 (1024秒轮询间隔)
	resp[2] = 0x0A
	
	// Precision: -20 (约微秒级精度)
	resp[3] = 0xEC
	
	// 根延迟和根离散设为0
	binary.BigEndian.PutUint32(resp[4:8], 0)
	binary.BigEndian.PutUint32(resp[8:12], 0)
	
	// 参考ID设为"LOCL"
	copy(resp[12:16], []byte{'L', 'O', 'C', 'L'})
	
	// 解析客户端发送时间 (T1)
	t1Sec := binary.BigEndian.Uint32(req[40:44])
	t1Frac := binary.BigEndian.Uint32(req[44:48])
	t1 := ntpToTime(t1Sec, t1Frac)
	
	// 获取当前时间作为接收时间 (T2)
	t2 := time.Now().UTC()
	
	// 设置参考时间戳（未使用）和原始时间戳（T1）、接收时间戳（T2）
	setNTPTime(resp[16:24], time.Time{}) // 参考时间戳 (未使用)
	setNTPTime(resp[24:32], t1)         // 原始时间戳 (T1)
	setNTPTime(resp[32:40], t2)         // 接收时间戳 (T2)
	
	// 在发送前记录传输时间 (T3)
	t3 := time.Now().UTC()
	setNTPTime(resp[40:48], t3)         // 传输时间戳 (T3)

	_, err := conn.WriteToUDP(resp, addr)
	if err != nil {
		log.Printf("error sending response to %v: %v", addr, err)
	}

	// 记录详细时间戳信息
	go logTimestamps(addr, t1, t2, t3)
}

// 将NTP时间转换为Go时间
func ntpToTime(sec uint32, frac uint32) time.Time {
	// 计算从1900年到1970年的秒数
	secondsSince1970 := int64(sec) - int64(ntpEpochOffset)
	
	// 将分数部分转换为纳秒 (0xFFFF FFFF = 2^32-1)
	nanoseconds := int64(frac) * 1e9 / (1 << 32)
	
	return time.Unix(secondsSince1970, nanoseconds).UTC()
}

// 设置NTP时间戳
func setNTPTime(b []byte, t time.Time) {
	var sec uint32
	var frac uint32
	
	if t.IsZero() {
		// 对于参考时间戳，使用0值
		sec = 0
		frac = 0
	} else {
		// 计算从1970年到1900年的秒数
		sec = uint32(t.Unix() + ntpEpochOffset)
		
		// 将纳秒转换为NTP分数部分 (0xFFFF FFFF = 2^32-1)
		frac = uint32((uint64(t.Nanosecond()) << 32) / 1e9)
	}
	
	binary.BigEndian.PutUint32(b[0:4], sec)
	binary.BigEndian.PutUint32(b[4:8], frac)
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