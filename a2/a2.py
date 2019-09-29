import redis
import random
import string
import click

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


def gen_commands(n):
    for _ in range(n):
        cmd, arg_count = random.choices(COMMANDS, k=1)[0]
        keys = random.choices(KEYS, k=1)
        if arg_count < 2:
            yield " ".join([cmd] + keys)
            continue
        val = KVS[keys[0]]

        yield " ".join([cmd] + [keys[0], val])


def longlive_checker(host, port, pipeline):
    rc = redis.StrictRedis(host=host, port=port,
                           socket_timeout=1)  # default 1s timeout
    while True:
        with rc.pipeline(transaction=False) as p:
            for cmd in gen_commands(pipeline):
                p.execute_command(cmd)
            try:
                p.execute()
            except Exception as e:
                print("fail to do execute for redis due %s" % (e))


@click.command()
@click.option("-s", "--server", default="127.0.0.1", help="default server ip")
@click.option("-p", "--port", default=6379, help="default test port")
@click.option("-P", "--pipeline", default=32, help="default pipeline size")
@click.option("-c",
              "--rand",
              default=10000,
              help="default random key-value count")
@click.option("-m",
              "--mode",
              default="longlive",
              help="avaliable mode is longlive|features")
def main(server, port, pipeline, rand, mode):
    gen_random_kvs(rand)
    if mode == "longlive":
        longlive_checker(server, port, pipeline)


if __name__ == "__main__":
    main()