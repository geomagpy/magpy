#!/usr/bin/python

import argparse
import getpass
import os
import sys
from cryptography.fernet import Fernet

sys.path.insert(1, '/home/leon/Software/magpy/')  # for testing
from magpy.opt import cred as mpcred


def main():
    description = (
        "Manage MagPy credentials stored in ~/.magpycred for databases, mail servers, "
        "and file transfers.\n\n"
        "ENCRYPTION & KEY MANAGEMENT:\n"
        "  - Encryption: If MAGPY_KEY is set, passwords are encrypted with Fernet (AES).\n"
        "                Otherwise, they fall back to URL-safe Base64 obfuscation.\n"
        "  - Generate  : python addcred.py --genkey\n"
        "  - Persist   : 'export MAGPY_KEY=...' only lasts for the current session.\n"
        "                To persist across reboots, add it to:\n"
        "                  * Shell   : ~/.bashrc or ~/.zshrc\n"
        "                  * Cronjobs: top of 'crontab -e' (cron ignores ~/.bashrc)\n"
        "                  * Services: Environment= or EnvironmentFile= in systemd unit\n"
        "  - Backup    : Store a backup of MAGPY_KEY in a safe place (e.g., password\n"
        "                manager). Without the key, encrypted credentials cannot be recovered.\n"
        "  - Security  : Omit -p/--password to enter passwords via hidden prompt."
    )
    epilog = (
        "EXAMPLES:\n"
        "  # Create transfer credentials (prompts securely for password):\n"
        "  python addcred.py -t transfer -c zamg -u max -a \"ftp://ftp.remote.ac.at\" -l 21\n\n"
        "  # Create database credentials:\n"
        "  python addcred.py -t db -c maindb -u max -d mydb -o localhost\n\n"
        "  # View all stored credentials:\n"
        "  python addcred.py -v\n\n"
        "  # Remove an existing entry:\n"
        "  python addcred.py -r zamg\n\n"
        "NOTE: Always put URLs or paths containing special characters in quotes!"
    )

    parser = argparse.ArgumentParser(
        prog="addcred.py",
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("-v", "--view", action="store_true",
                        help="view all existing credentials")
    parser.add_argument("--genkey", action="store_true",
                        help="generate a new valid Fernet key for MAGPY_KEY and exit")
    parser.add_argument("-r", "--remove", metavar="SHORTCUT",
                        help="remove an entry from the credential list by shortcut")
    parser.add_argument("-t", "--type", choices=["db", "transfer", "mail"],
                        help="type of credential: db, transfer, or mail")
    parser.add_argument("-c", "--shortcut",
                        help="unique shortcut name to access stored information")
    parser.add_argument("-u", "--user",
                        help="username for authentication")
    parser.add_argument("-p", "--password",
                        help="password (if omitted, you will be prompted securely)")
    parser.add_argument("-d", "--database",
                        help="database name (required for type 'db')")
    parser.add_argument("-o", "--host",
                        help="database host address (required for type 'db')")
    parser.add_argument("-s", "--smtp",
                        help="SMTP server address (required for type 'mail')")
    parser.add_argument("-a", "--address",
                        help="server/URL address (required for type 'transfer')")
    parser.add_argument("-l", "--port",
                        help="network port for transfer or mail")

    args = parser.parse_args()

    # 1. Generate new encryption key
    if args.genkey:
        key = Fernet.generate_key().decode()
        print("Generated new MAGPY_KEY (keep this secret!):")
        print(f"export MAGPY_KEY='{key}'")
        sys.exit(0)

    # 2. View credentials
    if args.view:
        mpcred.sc()
        sys.exit(0)

    # 3. Remove credential entry
    if args.remove:
        cont = mpcred.sc(quiet=True) or []
        shortcuts = [el[0] for el in cont]
        if args.remove in shortcuts:
            answer = input(f"You are going to delete the credential information for '{args.remove}' - continue (y/yes)? ")
            if answer.lower() in ["y", "yes"]:
                mpcred.dc(args.remove)
            else:
                print("Aborted.")
        else:
            print(f"addcred: did not find '{args.remove}' - aborting")
        sys.exit(0)

    # 4. Create / Update credentials - Validation
    if not args.shortcut:
        parser.error("Specify (and remember) a shortcut using -c / --shortcut.")
    if not args.type:
        parser.error("Specify a type (db, mail, or transfer) using -t / --type.")
    if not args.user:
        parser.error("Specify a user using -u / --user.")

    # Secure password prompt if -p was not provided on CLI
    password = args.password
    if password is None:
        if not os.environ.get("MAGPY_KEY"):
            print("Warning: MAGPY_KEY is not set. Password will only be Base64-obfuscated, not encrypted.")
        password = getpass.getpass(f"Enter password for '{args.shortcut}' (leave empty if none): ")

    # Dispatch by type
    if args.type == 'db':
        if not args.host:
            parser.error("Host required for type 'db' (-o / --host).")
        if not args.database:
            parser.error("Database name required for type 'db' (-d / --database).")
        mpcred.cc('db', args.shortcut, db=args.database, user=args.user, passwd=password, host=args.host)

    elif args.type == 'transfer':
        if not args.address:
            parser.error("Address required for type 'transfer' (-a / --address).")
        mpcred.cc('transfer', args.shortcut, user=args.user, passwd=password, address=args.address, port=args.port)

    elif args.type == 'mail':
        if not args.smtp:
            parser.error("SMTP address required for type 'mail' (-s / --smtp).")
        mpcred.cc('mail', args.shortcut, user=args.user, passwd=password, smtp=args.smtp, port=args.port)


if __name__ == "__main__":
    main()
