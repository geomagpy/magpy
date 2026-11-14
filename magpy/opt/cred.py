#!/usr/bin/env python
"""
DESCRIPTION:
    MagPy credentials functions:
    Methods for storing, accessing, displaying, and deleting user/password
    information for databases, mail servers, and file transfer accounts.

    Encryption & Security:
    - If the environment variable MAGPY_KEY is set, passwords are encrypted using
      symmetric Fernet (AES-128-CBC + HMAC-SHA256) encryption (prefixed with 'enc:').
    - If MAGPY_KEY is not set, passwords fall back to obfuscation
      (remaining backwards-compatible with legacy entries).
    - File permissions of ~/.magpycred are automatically restricted to 0600.

    Key Persistence & Best Practices:
    - A simple 'export MAGPY_KEY=...' is only valid for the current terminal session.
    - For permanent use across reboots, define MAGPY_KEY in ~/.bashrc (or ~/.zshrc),
      at the top of your crontab ('crontab -e', since cron ignores ~/.bashrc), or in
      your systemd service file.
    - Keep a secure backup of MAGPY_KEY outside the server (e.g., in a password manager).
      Lost keys cannot be recovered and render encrypted entries unreadable.

METHODS:
    - cc(): Create or update a credential entry in ~/.magpycred
    - lc(): Load and decrypt/decode a specific credential value
    - sc(): Show all stored credential entries
    - fc(): Find credential entry
    - dc(): Delete a credential entry by name

PARAMETERS:
    None

REQUIREMENTS:
    cryptography (for symmetric encryption via MAGPY_KEY)
    Independent of any other MagPy package.

EXAMPLE:
    from magpy.opt import cred as mpcred

    # 1. Generate a key once via CLI or Python:
    #    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    # 2. Export it permanently (e.g., in ~/.bashrc and crontab):
    #    export MAGPY_KEY='<generated_key>'

    # Creating credentials
    mpcred.cc('db', 'maindatabase', db='mydb', user='max', passwd='secret', host='myhost')
    mpcred.cc('mail', 'firstmail', user='moritz', smtp='schiller.ed', passwd='evenmoresecret')
    mpcred.cc('transfer', 'ftp1', user='friedrich', address='ftp.find.the.fish', passwd='somehowsecret')

    # Using credentials
    db_pass = mpcred.lc('maindatabase', 'passwd')
    mail_smtp = mpcred.lc('firstmail', 'smtp')

    exists = mpcred.fc('ftp1')

"""

import base64
import os
from os.path import expanduser
import pickle
import platform
import pwd
import sys
from cryptography.fernet import Fernet, InvalidToken


pyversion = platform.python_version()
PY3 = sys.version_info[0] >= 3


def _get_cred_path(path=None):
    """
    Resolve the path to the credentials file (~/.magpycred by default).
    """
    if path:
        return path
    sysuser = getuser()
    home = expanduser('~' + sysuser)
    return os.path.join(home, '.magpycred')


def _protect_password(passwd: str) -> str:
    """
    Encrypt a password using Fernet if MAGPY_KEY is set in the environment;
    otherwise fall back to URL-safe Base64 obfuscation.
    """
    if not passwd:
        return ''

    key = os.environ.get("MAGPY_KEY")
    if key:
        try:
            f = Fernet(key.encode())
            encrypted = f.encrypt(passwd.encode()).decode()
            return f"enc:{encrypted}"
        except ValueError:
            print("Credentials: Invalid MAGPY_KEY format - falling back to obfuscation!")
            return base64ify(passwd)
    else:
        return base64ify(passwd)


def _unprotect_password(stored_pwd: str) -> str:
    """
    Decrypt an 'enc:' prefixed password using MAGPY_KEY, or decode a
    Base64-obfuscated password for backwards compatibility.
    """
    if not stored_pwd:
        return ''

    if stored_pwd.startswith('enc:'):
        key = os.environ.get("MAGPY_KEY")
        if not key:
            print("Credentials: Encrypted password found, but MAGPY_KEY is not set!")
            return ''
        try:
            f = Fernet(key.encode())
            encrypted_data = stored_pwd[4:].encode()  # Strip 'enc:' prefix
            return f.decrypt(encrypted_data).decode()
        except (ValueError, InvalidToken):
            print("Credentials: Failed to decrypt password (invalid MAGPY_KEY or corrupted data)")
            return ''
    else:
        # Backwards compatibility for legacy / unencrypted Base64 entries
        try:
            data = base64.urlsafe_b64decode(stored_pwd)
        except Exception:
            data = base64.b64decode(stored_pwd)
        try:  # Python 2/3 compatibility
            data = data.decode()
        except AttributeError:
            pass
        return data


def saveobj(obj, filename):
    """
    Serialize and save a Python object to a binary file using pickle.
    """
    with open(filename, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def loadobj(filename):
    """
    Load and deserialize a Python object from a pickle file (Python 2/3 compatible).
    """
    if pyversion.startswith('2'):
        with open(filename, 'r') as f:
            return pickle.load(f)
    else:
        with open(filename, 'rb') as f:
            return pickle.load(f, encoding='latin1')


def getuser():
    """
    Determine the current system username, including fallbacks for cron jobs
    (LOGNAME) and default Linux user ID 1000.
    """
    sysuser = os.getenv("USER")
    if sysuser is None:
        print("Getuser: Running from crontab -- trying Logname to identify user")
        try:
            sysuser = os.getenv("LOGNAME").replace("LOGNAME=", "")
        except Exception:
            sysuser = None
        if sysuser is not None:
            print("Getuser: ... success - using", sysuser)

    if sysuser is None or sysuser == 'None':
        print("Getuser: Cannot identify user by standard procedures - switching to default uid 1000")
        sysuser = pwd.getpwuid(1000)[0]
        print("Getuser: now using", sysuser)
    return sysuser


def base64ify(bytes_or_str):
    """
    Encode a string or bytes object into a URL-safe Base64 string.
    """
    if PY3 and isinstance(bytes_or_str, str):
        input_bytes = bytes_or_str.encode('utf-8')
    else:
        input_bytes = bytes_or_str
    output_bytes = base64.urlsafe_b64encode(input_bytes)
    if PY3:
        return output_bytes.decode('ascii')
    return output_bytes


def cc(typus, name, user=None, passwd=None, smtp=None, db=None, address=None, remotedir=None, port=None, host=None):
    """
    Create or overwrite a credential entry in the credentials file.

    Parameters:
        typus (str): Credential type ('db', 'mail', or 'transfer').
        name (str): Unique identifier for the credential set.
        user (str): Username for authentication.
        passwd (str, optional): Password (encrypted if MAGPY_KEY is set).
        smtp (str, optional): SMTP server address (required for typus='mail').
        db (str, optional): Database name (required for typus='db').
        address (str, optional): Server/host address (required for typus='transfer').
        remotedir (str, optional): Remote directory path (reserved for future use).
        port (str/int, optional): Network port.
        host (str, optional): Database host address (required for typus='db').
    """
    user = user or ''
    passwd = passwd or ''
    db = db or ''
    address = address or ''
    host = host or ''
    smtp = smtp or ''
    port = port or ''

    credentials = _get_cred_path()

    try:
        dictslist = loadobj(credentials)
    except Exception:
        print("Credentials: Could not load credentials file - creating it ...")
        dictslist = []

    if any(d[0] == name for d in dictslist):
        print(f"Credentials: {typus} - Input for {name} is already existing - going to overwrite it")
        dictslist = [elem for elem in dictslist if elem[0] != name]

    if not user:
        print('Credentials: user missing')
        return
    if not passwd:
        print('Credentials: no password provided')

    pwd_protected = _protect_password(passwd)

    if typus == 'db':
        if not db:
            print('Credentials: database missing')
            return
        if not host:
            print('Credentials: host missing')
            return
        dictionary = {'user': user, 'passwd': pwd_protected, 'db': db, 'host': host}

    elif typus == 'mail':
        if not smtp:
            print('Credentials: smtp address missing')
            return
        dictionary = {'user': user, 'passwd': pwd_protected, 'smtp': smtp, 'port': port}

    elif typus == 'transfer':
        if not address:
            print('Credentials: address missing')
            return
        dictionary = {'user': user, 'passwd': pwd_protected, 'address': address, 'port': port}

    else:
        print(f"Credentials: Unknown typus '{typus}'")
        return

    dictslist.append([name, dictionary])
    saveobj(dictslist, credentials)

    try:
        os.chmod(credentials, 0o600)
    except OSError:
        pass

    print("Credentials: Added entry for", name)
    entries = [n[0] for n in dictslist]
    print("Credentials: Now containing entries for", entries)


def lc(dictionary, value, path=None, debug=False):
    """
    Load a specific credential value by entry name and key.

    Parameters:
        dictionary (str): Name of the credential entry (e.g., 'maindatabase').
        value (str): Key within the entry (e.g., 'user', 'passwd', 'host').
        path (str, optional): Custom path to the credentials file.
        debug (bool, optional): Print debug information if True.

    Returns:
        str or None: The requested value (decrypted/decoded if key contains 'passwd'),
                     or None if not found.
    """
    credentials = _get_cred_path(path)

    if debug:
        print("Accessing credential file:", credentials)

    try:
        dictslist = loadobj(credentials)
    except Exception:
        print("Credentials: Could not load file")
        return

    for d in dictslist:
        if d[0] == dictionary:
            if value not in d[1]:
                if debug:
                    print(f"Credentials: key '{value}' not found in '{dictionary}'")
                return
            if 'passwd' in value:
                return _unprotect_password(d[1][value])
            return d[1][value]

    print("Credentials: value/dict not found")
    return


def sc(quiet=False):
    """
    Show an overview of all stored credential entries.

    Parameters:
        quiet (bool, optional): If False, prints all entries to stdout.

    Returns:
        list or None: List of stored credential entries, or None if loading fails.
    """
    credentials = _get_cred_path()
    print("Credentials: Overview of existing credentials:")
    try:
        dictslist = loadobj(credentials)
    except Exception:
        print("Credentials: Could not load file")
        return

    if not quiet:
        for d in dictslist:
            print(d)
    return dictslist


def dc(name):
    """
    Delete a credential entry from the credentials file by name.

    Parameters:
        name (str): Unique identifier of the credential entry to remove.
    """
    credentials = _get_cred_path()
    try:
        dictslist = loadobj(credentials)
    except Exception:
        print("Credentials: Could not load credentials file - aborting ...")
        return

    if any(d[0] == name for d in dictslist):
        newdlist = [d for d in dictslist if d[0] != name]
        saveobj(newdlist, credentials)
        print("Credentials: Removed entry for", name)
        entries = [n[0] for n in newdlist]
        print("Credentials: Now containing entries for", entries)
    else:
        print(f"Credentials: Input for {name} not found - aborting")


def fc(name):
    """
    Find a credential entry from the credentials file by name.

    Parameters:
        name (str): Unique identifier of the credential entry.
    RETURNS:
        bool
    """
    credentials = _get_cred_path()
    try:
        dictslist = loadobj(credentials)
    except Exception:
        print("Credentials: Could not load credentials file - aborting ...")
        return

    if any(d[0] == name for d in dictslist):
        return True
    else:
        return False
