---
theme: [air, coffee]
sql:
  coauthor: data/coauthor.parquet
  paper: data/paper.parquet
  author: data/author.parquet
---

# Scientific collab timeline dashboard
## How do collabs and individual productivity coevolve over time?


<div class="grid grid-cols-4">
  <div class="card">
    <h2>Unique authors</h2>                                                                         
    <span class="big">${[...uniqCoAuthors].length}</span>
  </div>
  <div>
      ${form.a_nc === 'age_diff' ? 
      Plot.legend({
        color: {
          range: ["#FDE725FF", "#20A387FF", "#404788FF"], 
          domain: ["younger", "samge age", "older"]
        }
      }) : Plot.legend({ color })
    }
  </div>
  <div class="grid-colspan-2">
    ${formInput}
  </div>
</div>


```js
const formInput = Inputs.form({
  targets: Inputs.select([...uniqAuthors].map(d=>d.name), { multiple: 5, label: "Author" }),
  a_nc: Inputs.select(
    ["age_diff", "acquaintance", "shared_institutions", "institution"], 
    { label: "Select color", value: "age_diff" }
    ),
  a_r: Inputs.select(["yearly_collabo", "all_times_collabo"], { label: "Author node size", value: 'all_times_collabo' }),
  p_r: Inputs.select(["", "cited_by_count", "nb_coauthors"], { label: "Paper node size", value: "cited_by_count" }),
  yaxis: Inputs.select(["pub_date", "age_std"], { label: "Y-axis", value: "year" }),
  xaxis_ts: Inputs.select(["author_age", "pub_year"], { label: "Timeseries Y-axis", value: "pub_year" })
});

const form = Generators.input(formInput)
```

<div class="grid grid-cols-2">
<div>${resize((width) => 
Plot.plot({
        style: "overflow: visible;",
        width,
        height: 800,
        marginLeft: 35,
        color: {
          legend: false,
          type: "categorical",
        },
        fx: { label: null, padding: 0.03, axis: "top" },
        y: { 
          grid: true, 
          reverse: true, inset: 50, 
          domain: [new Date(min_y), new Date(max_y)] 
        }, 
        r: { range: [1, 10] },
        marks: [
          Plot.dot(coauthor, Plot.dodgeX("middle", {
            fx: "name",
            y: d => d[form.yaxis], 
            fill: d => {
              const v = form.a_nc === 'age_diff' ? age_bucket(d) : d[form.a_nc];
              return v == null ? "gray" : v;
            },
            r: d => d[form.a_r], 
            stroke: 'black', 
            strokeWidth: d => d[form.a_nc] === null ? 0.1 : .3, 
            fillOpacity: d => d[form.a_nc] === null ? 0.1 : .9, 
            title: d => `${d.coauth_name}`,
            tip: true
          }))
        ]
        }))
}
</div>
<div>${resize((width) => 
  Plot.plot({
        style: "overflow: visible;",
        width,
        height: 800,
        marginLeft: 35,
        y: { 
          grid: true, 
          reverse: true, inset: 50,
          domain: [new Date(min_y), new Date(max_y)]  }, 
        r: { range: [1, 10] },
        fx: { label: null, padding: 0.03, axis: "top" },
        marks: [
          Plot.dot(paper, Plot.dodgeX("middle", {
            fx: "ego_aid",
            y: d => d[form.yaxis], 
            fill: 'grey',
            r: d => p_r(d), 
            stroke: 'black', 
            strokeWidth: 0.3, 
            fillOpacity: 0.7, 
            title: d => `Title: ${d.title}\n#coauthors: ${d.authors.split(", ").length}\n${d.pub_year}\n#citations: ${d.cited_by_count}`,
            tip: true
          }))
        ]
}))
}
</div>
</div>


```js
const selected_authors = form.targets.length > 0 ? form.targets[0] : 'Molly Stanley'
```

```js
const min_y = Math.min(d3.min([...coauthor].map(d=>d[form.yaxis])),d3.min([...paper].map(d=>d[form.yaxis])))
const max_y = Math.max(d3.max([...coauthor].map(d=>d[form.yaxis])),d3.max([...paper].map(d=>d[form.yaxis])))
```

## Coauthor table

```sql id=coauthor
SELECT age_std::DATE as age_std, pub_date::DATE as pub_date, * FROM coauthor 
WHERE name = ${selected_authors}
ORDER BY pub_year
```

```js
Inputs.table(coauthor)
```

## Paper table

```sql id=paper
SELECT a.age_std::DATE as age_std, *
FROM paper p
LEFT JOIN author a
ON p.ego_aid = a.aid AND p.pub_year = a.pub_year
WHERE name = ${selected_authors}
ORDER BY a.pub_year
```

```js
Inputs.table(paper)
```


```sql id=uniqAuthors
SELECT DISTINCT name FROM coauthor ORDER BY name
```

```sql id=uniqCoAuthors
SELECT DISTINCT coauth_name FROM coauthor WHERE name = ${selected_authors} 
```

```js
function p_r(d) {
        switch (form.p_r) {
        case 'nb_coauthor':
          return d.author === null ? 0 : d.author.split(", ").length;
        case '':
          return 0.1;
        case 'cited_by_count':
          return d['cited_by_count'];
        case 'nb_coauthors':
          return d.authors.split(", ").length;
      }
    }
```

```js
function age_bucket(d) {
          switch (true) {
              case d.age_diff > 7 : 
                return "#404788FF"
              case d.age_diff <= 7 && d.age_diff >= -7:
                return "#20A387FF"
              case d.age_diff < -7:
                return "#FDE725FF"
            };
  }
```

<!-- LEGEND COAUTHOR -->

```js
const domain = [...new Set([...coauthor]
  .map(d => form.a_nc === 'age_diff' ? age_bucket(d) : d[form.a_nc])
  .filter(v => v != null)
)];
```

```js
const range = d3.schemeTableau10.slice(0, domain.length);
```

```js
const color = { type: "categorical", domain, range };
```