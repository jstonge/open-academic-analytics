

```js
const db = DuckDBClient.of({data: await FileAttachment("./joined.csv")});
```
```js
const data = db.query("SELECT * FROM data")
```
```js
Inputs.table(data)
```

${[...data].length}