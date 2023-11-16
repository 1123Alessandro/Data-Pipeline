from pandas import read_csv
import pyperclip as pc

# Read csv file in workspace then convert to a latex tabular then copy to clipboard
def lats(show=False, s='data.csv'):
    ar = read_csv(s, dtype=str)
    ss = ar.to_latex(index=False)
    pc.copy(ss)
    print(pc.paste() if show else f"Data copied from {s}")
    return(ar)
