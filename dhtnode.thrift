service DhtNode {
        string write(1: string fileName, 2: string contents),
        string read(1: string fileName),
        void updateDHT(1:string nodes),
        string getdhtstructure(),
}
