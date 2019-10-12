import redis
import random
import string
import click
import memcache

CHARS = string.ascii_uppercase + string.ascii_lowercase + string.digits


def gen_str(N):
    return ''.join(random.choices(CHARS, k=N))


COMMANDS = [
    ("GET", 1),
    ("SET", 2),
    ("MGET", 1),
    ("MSET", 2),
]

KVS = {}
KEYS = []


def gen_random_kvs(n, key_size=8, value_size=4):
    global KVS, KEYS
    for _ in range(n):
        key = gen_str(key_size)
        val = gen_str(value_size)
        KVS[key] = val
    KEYS = list(KVS.keys())


def gen_redis_commands(n):
    for _ in range(n):
        cmd, arg_count = random.choices(COMMANDS, k=1)[0]
        keys = random.choices(KEYS, k=1)
        if arg_count < 2:
            yield " ".join([cmd] + keys)
            continue
        val = KVS[keys[0]]

        yield " ".join([cmd] + [keys[0], val])


def longlive_rc_checker(host, port, pipeline):
    rc = redis.StrictRedis(host=host, port=port,
                           socket_timeout=1)  # default 1s timeout
    while True:
        with rc.pipeline(transaction=False) as p:
            for cmd in gen_redis_commands(pipeline):
                p.execute_command(cmd)
            try:
                p.execute()
            except Exception as e:
                print("fail to do execute for redis due %s" % (e))


def longlive_mc_checker(host, port):
    mc = memcache.Client(["{}:{}".format(host, port)])
    count = 0
    while True:
        count += 1
        key = random.choices(KEYS, k=1)[0]
        val = KVS[key]
        if count % 2 == 0:
            mc.set(key, val)
        else:
            nval = mc.get(key)
            if nval:
                assert val == nval


@click.command()
@click.option("-s", "--server", default="127.0.0.1", help="default server ip")
@click.option("-p", "--port", default=6379, help="default test port")
@click.option("-P", "--pipeline", default=32, help="default pipeline size")
@click.option("-c",
              "--rand",
              default=10000,
              help="default random key-value count")
@click.option("-m", "--mode", default="redis", help="support only mc|redis")
@click.option("-d", "--dsize", default=32, help="data size, default is 32")
def main(server, port, pipeline, rand, mode, dsize):
    gen_random_kvs(rand, value_size=dsize)

    if mode == "redis":
        longlive_rc_checker(server, port, pipeline)
    elif mode == "mc":
        longlive_mc_checker(server, port)


if __name__ == "__main__":
    main()
