from ta.scanner import Scanner
import os


if __name__ == "__main__":
    scanner = Scanner()
    scanner.sum_df()
    scanner.print_dfs()
    scanner.save_df(os.path.join('data', 'indicators.pkl'))