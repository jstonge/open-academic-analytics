---
sql:
  coauthor: data/coauthor.parquet
  paper: data/paper.parquet
  author: data/author.parquet
  training: data/training_data.parquet
---

# Scientific collab timeline dashboard
## How do collabs and individual productivity coevolve over time?

<div class="grid grid-cols-4">
  <div class="card">
    <h2>Unique authors</h2>                                                                         
    <span class="big">${[...uniqCoAuthors].length}</span>
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

<div class="warning">There might be some errors in the data, e.g. authors wrongly gets attributed a paper or I mislabeled group status of an author. If this is the case, feel free to update the following  <a href="https://docs.google.com/spreadsheets/d/1LYoj01Wnfhd8SPNZXg1bg1jjxE9TSZ-pCKhoqhD0uWo/edit#gid=0">excel sheet</a></div>
<div class="grid grid-cols-2">
  <div>In the lineplot on the right, we show the changing rates for a selected author. Right now, we force the model to find a switchpoint in between an early and late rate. We still need to do model selection to determine if the switchpoint is necessary or not.<br><br> ${formInput}</div>
  <div >${resize((width) => 
  Plot.plot({
    title: "switch point for selected authors",
    width,
    height: 320,
    y: {grid: true, label: "Collabo younger author"}, 
    color: {
      legend: true, 
      range: ["red", "black", "lightgrey"], 
      domain: ["younger", "same_age", "older"]
    },
    marks: [
      Plot.frame(),
      Plot.lineY(trainingEgo, { x: form.xaxis_ts, y: "changing_rate", 
                strokeWidth: 1, strokeDasharray: 3, stroke: "red",
              },     
      ),
      Plot.dotY(
        trainingEgo, { x: form.xaxis_ts, y: "younger", stroke: "red"},     
      ),
      Plot.dotY(
        trainingEgo, { x: form.xaxis_ts, y: "same_age", stroke: "black"},     
      ),
      Plot.dotY(
        trainingEgo, { x: form.xaxis_ts, y: "older", stroke: "lightgrey"},     
      ),
      Plot.dotY(trainingEgo, Plot.selectLast({
        x: form.xaxis_ts, y: "changing_rate", fill: d => d["has_research_group"] === "1.0" ? "green" : "lightgrey",  
        r: 10, symbol: "star", fillOpacity: 0.7, stroke: "black"
      })),
      Plot.text(trainingEgo, Plot.selectLast(
        {x: form.xaxis_ts, y: "changing_rate", text: d => d["has_research_group"] == "1.0" ? "Has\nresearch\ngroup" : "No\ngroup", dx: 40
        }))
    ]
  })
  )}
  </div>
</div>


<div class="grid grid-cols-2">
<div>${resize((width) => 
Plot.plot({
        style: "overflow: visible;",
        width,
        height: 800,
        marginLeft: 35,
        color: {
          legend: true
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
            fill: d => form.a_nc === 'age_diff' ? age_bucket(d) : d[form.a_nc],
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
${form.a_nc === 'age_diff' ? 
  Plot.legend({
    color: {
      range: ["#FDE725FF", "#20A387FF", "#404788FF"], 
      domain: ["younger", "samge age", "older"]
    }
  }) : ""
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

---

Features to examines:
<div class="grid grid-cols-4">
  <div class="card"><br><br><br><br><br><br>
    <figure>
      <img src="https://raw.githubusercontent.com/jstonge/hello-research-groups/main/static/friendship.svg" alt="growth" style="width:100%">
    </figure> 
    <br><br><br><br><br><br><br>The power of Two
  </div>
  <div>
    <figure>
      <img src="https://raw.githubusercontent.com/jstonge/hello-research-groups/main/static/width.svg" 
      alt="width" style="width:100%">
    </figure> 
    Average width + Max width
  </div>
</div>

---

```js
const selected_authors = form.targets.length > 0 ? form.targets[0] : 'Aaron Clauset'
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

## training table

```sql id=trainingEgo
  SELECT DISTINCT name, changing_rate, college, author_age, younger, 
      same_age, older, has_research_group::CHAR as has_research_group, pub_year
  FROM training 
  WHERE name = ${selected_authors}
  ORDER BY author_age
```

```sql
SELECT * FROM training WHERE name = ${selected_authors} ORDER BY author_age
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
