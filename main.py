# main file

import ConnectDatabase as db









def main():
    conn = db.connect()
    db.reset(conn)


if __name__ == "__main__":
    main()