# io-fixed-throughput

I/O with fixed throughput.

## Build

First install and configure conan:

```shell
pip3 install --upgrade conan
conan profile detect
```

Add my conan server to remote:

```shell
conan remote add seekstar http://nuc.seekstar.top:9300
```

Then build:

```shell
conan build .
```

The results are in `build/Release`.
