# skytable-go
Skytable client driver for Go (WIP)

## Introduction

This library is a client for the free and open-source NoSQL database
[Skytable](https://github.com/skytable/skytable). First, go ahead and install Skytable by
following the instructions [here](https://docs.skytable.io/getting-started). 
This version of the library was tested with the latest Skytable release
(release [0.7.5](https://github.com/skytable/skytable/releases/v0.7.5)).

## Installation

```go get github.com/satvik007/skytable-go```

## Features
- (Will) support skytable 0.7.5 and above
- Connection pooling
- Automatic reconnection
- Automatic retry on error, timeout, and connection loss

## Contributing

Open-source, and contributions are always welcome! For ideas and suggestions,
[create an issue on GitHub](https://github.com/satvik007/skytable-go/issues/new) and for patches,
fork and open those pull requests [here](https://github.com/satvik007/skytable-go)!

## License

This client library is distributed under the permissive
[Apache-2.0 License](https://github.com/satvik007/skytable-go/blob/next/LICENSE).

## Copyrights

Many parts of this codebase comes from the [go-redis](https://github.com/go-redis/redis) client.
Originally licensed under the BSD-2-Clause license.
Copyrights belong to their respective authors.

