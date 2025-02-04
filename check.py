with open("scheduler") as f:
    sch = f.read()

with open("cluster") as f:
    cluster = f.read()

with open("shutff") as f:
    shutff = f.read()

# read_message(): read message ; HASH: -599833624
# write_message(): writing message ; HASH: -1390068153
def parse(s) -> tuple[str, str] | None:
    if s.startswith("read_message"):
        id = s.split("HASH: ")[1]
        return (id, 0)
    if s.startswith("write_message"):
        id = s.split("HASH: ")[1]
        return (id, 1)
    return None

scheduler = [[], []]
cluster_ = [[], []]
shutff_ = [[], []]

for line in sch.split("\n"):
    res = parse(line)
    if res is not None:
        id, op = res
        scheduler[op].append(id)

for line in cluster.split("\n"):
    res = parse(line)
    if res is not None:
        id, op = res
        cluster_[op].append(id)

for line in shutff.split("\n"):
    res = parse(line)
    if res is not None:
        id, op = res
        shutff_[op].append(id)

print(f"Scheduler: {scheduler}")
print(f"Cluster: {cluster_}")
print(f"Shutff: {shutff_}")

all_read = scheduler[0] + cluster_[0] + shutff_[0]
all_write = scheduler[1] + cluster_[1] + shutff_[1]

# diff = all_read.difference(all_write)

# print(f"Diff: {diff}")
