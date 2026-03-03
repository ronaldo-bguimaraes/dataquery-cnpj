# Teste com input comprimido
 - tamanho real do csv: 2047 MB
 - tamanho do output em parquet: 811 MB
 - quantidade de linhas: 26,228,745.00

## Resultados da conversao bruta de csv -> parquet

### Pandas para leitura de csv (read_csv com chunksize) e PyArrow para escrita de parquet
 - iterações: 26
 - tempo: 162.00s

### PyArrow para leitura de csv e escrita de parquet
 - iterações: 2000
 - tempo: 83.14s
 - 48% mais rápido em relação a leitura com pandas

### Polars para leitura de csv (scan_csv modo lazy) e PyArrow para escrita de parquet
 - Sem resultado, Polars suporta apenas os encodings utf8 e utf8-lossy
