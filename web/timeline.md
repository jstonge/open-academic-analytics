---
theme: wide
toc: false
sql:
  coauthor: coauthor.parquet
  paper: paper.parquet
  author: author.parquet
---

# Explore faculties timeline

<div class="warning" label="⚠️ Known data issues ⚠️">
 <ul>
  <li>When ego's earliest publication is misclassified, it creates issues such as all collaborators being younger than the targeted author. We fix this problem for relevant authors by hardcoding fir publication year <a href="https://docs.google.com/spreadsheets/d/1LYoj01Wnfhd8SPNZXg1bg1jjxE9TSZ-pCKhoqhD0uWo/edit?gid=1748010441#gid=1748010441">in the excel sheet</a>.</li>
  <li>Some faculties are still missing.</li>
</ul> 
</div>

```js
// Independent input controls
const facultySelectInput = Inputs.select([...uniqAuthors].map(d=>d["Faculty Name"]), { 
  multiple: 17, 
  label: "Select Faculty Members" 
});
const selectedFaculty = Generators.input(facultySelectInput);

const nodeColorInput = Inputs.select(
  ["Age Difference", "Acquaintance", "Shared Institutions", "Institution"], 
  { label: "Node Color By", value: "Age Difference" }
);
const nodeColorBy = Generators.input(nodeColorInput);

const authorNodeSizeInput = Inputs.select(
  ["Yearly Collaborations", "Total Collaborations"], 
  { label: "Author Node Size", value: 'Total Collaborations' }
);
const authorNodeSizeBy = Generators.input(authorNodeSizeInput);

const paperNodeSizeInput = Inputs.select(
  ["", "Citations", "Number of Coauthors"], 
  { label: "Paper Node Size", value: "Citations" }
);
const paperNodeSizeBy = Generators.input(paperNodeSizeInput);

const timelineAxisInput = Inputs.select(
  ["Publication Date", "Standardized Age"], 
  { label: "Timeline Y-Axis", value: "Publication Date" }
);
const timelineAxis = Generators.input(timelineAxisInput);

const timeseriesAxisInput = Inputs.select(
  ["author_age", "pub_year"], 
  { label: "Timeseries Y-Axis", value: "pub_year" }
);
const timeseriesAxis = Generators.input(timeseriesAxisInput);
```

```js
const nb_coauthors = [...uniqCoAuthors].length
```

<div class="grid grid-cols-4">
  <div class="grid-colspan-1">
    <div class="card">
      <h2>Unique authors in the dataset</h2>                                                                         
      <span class="big">${[...uniqAuthors].length}</span>
    </div>
    <div class="card">
      <h2>Unique Co-authors of ${selectedAuthor}</h2>                                                                         
      <span class="big">${nb_coauthors}</span>
    </div>
    <div>
      <div style="margin-bottom: 4px;">${facultySelectInput}</div>
      <div style="margin-bottom: 4px;">${nodeColorInput}</div>
      <div style="margin-bottom: 4px;">${authorNodeSizeInput}</div>
      <div style="margin-bottom: 4px;">${paperNodeSizeInput}</div>
      <div style="margin-bottom: 4px;">${timelineAxisInput}</div>
      <!-- <div style="margin-bottom: 4px;">${timeseriesAxisInput}</div> -->
    </div>
  </div>
  <div class="grid-colspan-3">
    <div class="grid grid-cols-2">
      <div class="card">
        <h3>${selectedAuthor}'s Coauthors</h3>
        ${resize((width) => 
        Plot.plot({
                style: "overflow: visible;",
                width,
                height: nb_coauthors > 550 ? nb_coauthors > 1000 ? 1200 : 950 : 650,
                marginLeft: 35,
                color: {
                  legend: false,
                  type: "categorical",
                },
                fx: { label: null, padding: 0.03, axis: null },
                y: { 
                  grid: true, 
                  reverse: true, 
                  domain: [new Date(minYear), new Date(maxYear)] 
                }, 
                r: { range: [1, 10] },
                marks: [
                  Plot.dot(coauthor, Plot.dodgeX("middle", {
                    fx: "Faculty Name",
                    y: d => d[timelineAxis], 
                    fill: d => {
                      const v = nodeColorBy === 'Age Difference' ? age_bucket(d) : d[nodeColorBy];
                      return v == null ? "gray" : v;
                    },
                    r: d => d[authorNodeSizeBy], 
                    stroke: 'black', 
                    strokeWidth: d => d[nodeColorBy] === null ? 0.1 : .3, 
                    fillOpacity: d => d[nodeColorBy] === null ? 0.1 : .9, 
                    title: d => `${d["Coauthor Name"]}`,
                    tip: true
                  }))
                ]
                }))
        }
        <div style="margin-top: 10px;">
          ${nodeColorBy === 'Age Difference' ? 
              Plot.legend({
                color: {
                  range: ["#FDE725FF", "#20A387FF", "#404788FF"], 
                  domain: ["younger", "same age", "older"]
                }
              }) : Plot.legend({ color })
            }
        </div>
      </div>
      <div class="card">
        <h3>Papers</h3>
        ${resize((width) => 
          Plot.plot({
                style: "overflow: visible;",
                width,
                height: nb_coauthors > 550 ? nb_coauthors > 1000 ? 1200 : 950 : 650,
                marginLeft: 35,
                y: { 
                  grid: true, 
                  reverse: true, 
                  domain: [new Date(minYear), new Date(maxYear)]  }, 
                r: { range: [1, 10] },
                fx: { label: null, padding: 0.03, axis: null },
                marks: [
                  Plot.dot(paper, Plot.dodgeX("middle", {
                    fx: "Author ID",
                    y: d => d[timelineAxis], 
                    fill: 'grey',
                    r: d => calculatePaperNodeSize(d), 
                    stroke: 'black', 
                    strokeWidth: 0.3, 
                    fillOpacity: 0.7, 
                    wrap: 400,
                    title: d => `Title: ${d["Paper Title"]}\n#coauthors: ${d["Coauthors"].split(", ").length}\n${d["Publication Year"]}\ndoi: ${d["DOI"]}\n#citations: ${d["Citations"]}`,
                    tip: true
                  }))
                ]
        }))
        }
        <div style="height: 35px; margin-top: 10px;"></div>
      </div>
    </div>
  </div>
</div>

```js
const selectedAuthor = selectedFaculty.length > 0 ? selectedFaculty[0] : 'Peter Sheridan Dodds'
```

```js
const minYear = Math.min(d3.min([...coauthor].map(d=>d[timelineAxis])),d3.min([...paper].map(d=>d[timelineAxis])))
const maxYear = Math.max(d3.max([...coauthor].map(d=>d[timelineAxis])),d3.max([...paper].map(d=>d[timelineAxis])))
```

<!-- ## Coauthor table -->

```sql id=coauthor
SELECT 
  age_std::DATE as "Standardized Age", 
  pub_date::DATE as "Publication Date", 
  pub_year as "Publication Year",
  aid as "Author ID",
  institution as "Institution",
  name as "Faculty Name",
  author_age as "Author Age",
  first_pub_year as "First Publication Year",
  last_pub_year as "Last Publication Year",
  yearly_collabo as "Yearly Collaborations",
  all_times_collabo as "Total Collaborations",
  acquaintance as "Acquaintance",
  shared_institutions as "Shared Institutions",
  coauth_aid as "Coauthor ID",
  coauth_name as "Coauthor Name",
  coauth_age as "Coauthor Age",
  coauth_min_year as "Coauthor First Year",
  age_diff as "Age Difference",
  age_bucket as "Age Bucket"
FROM coauthor 
WHERE name = ${selectedAuthor}
ORDER BY pub_year
```

<!-- ## Paper table -->

```sql id=paper
SELECT 
  a.age_std::DATE as "Standardized Age",
  p.index as "Index",
  p.ego_aid as "Author ID",
  p.name as "Faculty Name",
  p.pub_date as "Publication Date",
  p.pub_year as "Publication Year",
  p.title as "Paper Title",
  p.cited_by_count as "Citations",
  p.doi as "DOI",
  p.wid as "Work ID",
  p.authors as "Coauthors",
  p.work_type as "Publication Type",
  p.ego_age as "Author Age",
  p.nb_coauthors as "Number of Coauthors",
  a.aid as "Author Identifier",
  a.display_name as "Display Name",
  a.institution as "Institution",
  a.first_pub_year as "First Publication Year",
  a.last_pub_year as "Last Publication Year",
  a.author_age as "Author Age at Publication"
FROM paper p
LEFT JOIN author a
ON p.ego_aid = a.aid AND p.pub_year = a.pub_year
WHERE name = ${selectedAuthor}
ORDER BY a.pub_year
```

```sql id=uniqAuthors
SELECT DISTINCT name as "Faculty Name" FROM coauthor ORDER BY name
```

```sql id=uniqCoAuthors
SELECT DISTINCT coauth_name as "Coauthor Name" FROM coauthor WHERE name = ${selectedAuthor} 
```

```js
function calculatePaperNodeSize(d) {
        switch (paperNodeSizeBy) {
        case 'nb_coauthor':
          return d["Coauthors"] === null ? 0 : d["Coauthors"].split(", ").length;
        case '':
          return 0.1;
        case 'Citations':
          return d['Citations'];
        case 'Number of Coauthors':
          return d["Coauthors"].split(", ").length;
      }
    }
```

```js
function age_bucket(d) {
          switch (true) {
              case d["Age Difference"] > 7 : 
                return "#404788FF"
              case d["Age Difference"] <= 7 && d["Age Difference"] >= -7:
                return "#20A387FF"
              case d["Age Difference"] < -7:
                return "#FDE725FF"
            };
  }
```

<!-- LEGEND COAUTHOR -->

```js
const colorDomain = [...new Set([...coauthor]
  .map(d => nodeColorBy === 'Age Difference' ? age_bucket(d) : d[nodeColorBy])
  .filter(v => v != null)
)];
```

```js
const colorRange = d3.schemeTableau10.slice(0, colorDomain.length);
```

```js
const color = { type: "categorical", domain: colorDomain, range: colorRange };
```
