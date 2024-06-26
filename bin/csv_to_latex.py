import argparse

parser = argparse.ArgumentParser(description="Convert a csv to a latex table")
parser.add_argument("input_filename", action="store", type=str,help="the csv to convert to latex")
parser.add_argument("output_filename", action="store", type = str, help = "the path to write the output txt file to")
args = parser.parse_args()

with open(args.input_filename, 'r') as f:
    lines = f.readlines()

lines = ["\t\t"+l.replace('\n',' \\\\\n').replace(","," & ") for l in lines]

with open(args.output_filename, 'w') as f:
    cs = "c" * (len(lines[0].split("&")))
    f.write("\\begin{table}[h]\n")
    f.write("    \\centering\n")
    f.write("    \\begin{tabular*}{\\textwidth}{@{\\extracolsep{\\fill}}"+cs+"}\n")
    f.write("    \\hline\n")
    f.writelines(lines[0])
    f.write("    \\hline\n")
    f.writelines(lines[1:])
    f.write("     \\bottomrule\n")
    f.write("    \\end{tabular*}\n")
    f.write("    \\caption{Caption here}\n")
    f.write("    \\label{tab:label}\n")
    f.write("\\end{table}\n")
print("Done!")


# \begin{table}[h]
#     \centering
#     \begin{tabular*}{\textwidth}{@{\extracolsep{\fill}}cccccc}
#     \hline
#         ISO & Y (He) & Z & Zeff & [Fe/H] & [a/Fe] \\
#     \hline
#         M05 & 0.2571 & 7.4439E-03 & 5.4210E-03 & -0.49 & 0.20 \\ 
#         M10 & 0.2489 & 2.3966E-03 & 1.7453E-03 & -0.99 & 0.20 \\ 
#         M15 & 0.2462 & 7.6184E-04 & 5.5481E-04 & -1.49 & 0.20 \\ 
#         M20 & 0.2454 & 2.4080E-04 & 1.7536E-04 & -1.99 & 0.20 \\ 
#         M25 & 0.2451 & 7.6310E-05 & 5.5572E-05 & -2.49 & 0.20 \\ 
#         P00 & 0.2863 & 2.5504E-02 & 1.8573E-02 & 0.07 & 0.20 \\ 
#         P03 & 0.3202 & 4.6409E-02 & 3.3797E-02 & 0.37 & 0.20 \\ 
#         P05 & 0.3537 & 6.7081E-02 & 4.8851E-02 & 0.57 & 0.20 \\
#      \bottomrule
#     \end{tabular*}
#     \caption{The model isochrone families used for fitting. Ages of individual isochrones ranged between 1 and 14 Gyr in increments of 0.5 Gyr.}
#     \label{tab:iso}
# \end{table}