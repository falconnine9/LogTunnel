# LogTunnel
A tool to tunnel log files into postgreSQL database entries.

## Configuring a tunnel
(Please note that any tunneled file will have all its contents deleted)

1. Open your config file (probably in /etc/logtunnel/config.json)
2. Create a new dictionary entry in the "tunnels" section
3. Set each of the required tunnel settings (config.example.json has an example of the required settings)
4. Restart the LogTunnel service

## LogTunnel formatting
For example, say you have the following format:

```[$timestamp$] $address$ - $request_uri$ | $status$```

And a log entry is submitted as

```[2022-03-17 13:34:17] 127.0.0.1 - /favicon.ico | 200```

Would become the following entry in a postgreSQL database

| timestamp           | address   | request_uri  | status |
|:-------------------:|:---------:|:------------:|:------:|
| 2022-03-17 13:34:17 | 127.0.0.1 | /favicon.ico | 200    |
