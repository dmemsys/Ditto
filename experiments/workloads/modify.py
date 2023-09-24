# a python script that transfers the original 
# twitter workload format into the required format

for i in range(1, 55):
    o_fname = "cluster{:03d}".format(i)
    with open(o_fname, "r") as f:
        lines = f.readlines()
    new_lines = []
    for l in lines:
        sl = l.split(',')
        nl = ' '.join(sl)
        new_lines.append(nl)
    n_fname = "twitter{:03d}".format(i)
    with open(n_fname, 'w') as f:
        f.writelines(new_lines)