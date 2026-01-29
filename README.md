# NTP Server in Go

This project implements a simple NTP (Network Time Protocol) server using the Go programming language. It allows devices on a local network to query the server's date and time, making it ideal for lab environments, testing, or isolated networks.

## Features

- **Basic NTP server:** Handles standard NTP requests (UDP port 123) and responds with the system's local time.
- **Access log:** Records each query made to the server, including date and time, client IP, and hostname (when possible), in a `ntp_access.log` file.
- **Simple and educational code:** Easy to understand, modify, and expand for local or lab needs.

## Warnings

> **Important:** This NTP server is intended for use only in controlled environments, local networks, or for educational purposes. It does not use authentication, does not synchronize with external time sources, and does not implement all standard NTP features.  
> **Do not use in production environments or where time accuracy and security are critical.**

## How to Build

The project uses a `Makefile` to simplify the build process for different platforms.

### Prerequisites

- Go 1.16 or higher installed ([download here](https://go.dev/dl/))
- `make` installed (available natively on most Unix systems; on Windows, you can use [Gow](https://github.com/bmatzelle/gow) or [GnuWin](http://gnuwin32.sourceforge.net/packages/make.htm))

### Build using Makefile
Use `make all` to compile the project for the main platforms.

To compile for a specific platform only, use `make {platform}`.  
Available options: `linux`, `windows`, `mac`, `mac-arm`.

To remove generated binaries, run `make clean`.

## How to Use

1. Compile as above for your platform.
2. Run as administrator/root (port 123 is privileged):
   ```sh
   sudo ./ntp-server
   ```
   On Windows, run the terminal as administrator.

3. NTP clients can now query the server time using the IP/machine where `ntp-server` is running.

4. Check the `ntp_access.log` file to view access logs.

## Log Example

```
2025/05/24 16:01:12 Time queried at 2025-05-24 16:01:12 by 192.168.0.10 (hostname: workstation-10.local)
2025/05/24 16:05:43 Time queried at 2025-05-24 16:05:43 by 192.168.0.23 (hostname: -)
```

---

Made with ❤️ by Bi-Ga Tech