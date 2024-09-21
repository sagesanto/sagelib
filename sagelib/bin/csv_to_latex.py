#! python

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Convert a csv to a latex table")
    parser.add_argument("input_filename", action="store", type=str,help="the csv to convert to latex")
    parser.add_argument("output_filename", action="store", type = str, help = "the path to write the output txt file to")
    args = parser.parse_args()

    with open(args.input_filename, 'r') as f:
        lines = f.readlines()

    lines = ["\t\t"+l.replace('\n',' \\\\\n').replace("&","\&").replace("$","\$").replace("%","\%").replace(","," & ") for l in lines]

    with open(args.output_filename, 'w') as f:
        cs = "c" * (len(lines[0].split("&")))
        f.write("\\begin{table}[h]\n")
        f.write("    \\centering\n")
        f.write("    \\begin{tabular*}{\\textwidth}{@{\\extracolsep{\\fill}}"+cs+"}\n")
        f.write("    \\hline\n")
        f.writelines(lines[0])
        f.write("    \\hline\n")
        f.writelines(lines[1:])
        f.write("     \\\\\n")
        f.write("     \\bottomrule\n")
        f.write("    \\end{tabular*}\n")
        f.write("    \\caption{Caption here}\n")
        f.write("    \\label{tab:label}\n")
        f.write("\\end{table}\n")
    print("Done!")

if __name__ == "__main__":
    main()