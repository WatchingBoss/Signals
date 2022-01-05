from ta.scanner import Scanner


if __name__ == "__main__":
    scanner = Scanner()
    scanner.fill_all_stocks()
    scanner.print_dfs()