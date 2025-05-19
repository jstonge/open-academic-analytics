
import os
import pandas as pd
import numpy as np
import arviz as az
import seaborn as sns
from cmdstanpy import CmdStanModel
from pathlib import Path
import argparse
import matplotlib.pyplot as plt
from  matplotlib.ticker import FuncFormatter


def parse_args():
    parser = argparse.ArgumentParser("Data Downloader")
    parser.add_argument(
        "-i",
        "--input",
        type= Path,
        help="JSONlines file with urls and hashes",
        required=True,
    )
    parser.add_argument(
        "-m",
        "--model",
        type= Path,
        help="JSONlines file with urls and hashes",
        required=True,
    )
    parser.add_argument(
        "-o", "--output", type=Path, help="output directory", required=True
    )
    return parser.parse_args()



def main():
    # INPUT_DIR = Path("../../docs/data")
    # OUTPUT_DIR = Path("../../docs/data")
    # MDOEL_DIR = Path("../../docs/data")
    args = parse_args()
    INPUT_DIR = args.input
    OUTPUT_DIR = args.output
    MODEL_DIR = args.model
    
    dat = pd.read_parquet(INPUT_DIR / 'training_data.parquet')

    model = CmdStanModel(stan_file= MODEL_DIR / "change_point02.stan")

    out={}
    for name in dat.name.unique():
        # name=dat.name.unique()[23]
        dat_author = dat[dat['name'] == name]
        
        # sns.scatterplot(x='pub_year', y='younger', data=dat_author)
        # sns.lineplot(x='pub_year', y='younger', data=dat_author)

        nb_collabs = dat_author.younger.astype(int)
        years = dat_author.pub_year.tolist()

        fit = model.sample(data={"T" : len(years), "D" : nb_collabs.to_list()})
        fit_az = az.InferenceData(posterior=fit.draws_xr())
        post = fit_az.posterior
        trace = post.stack(draws=("chain", "draw"))
        
        # fit.summary()

        # az.plot_trace(fit_az, var_names=('e', 'l'))
        
        ys = [ np.exp(_[-1]) for _ in trace['lp'].to_numpy() ] 
        ys = np.round(ys/np.sum(ys), 3)

        # plt.figure(figsize=(7, 4))
        # df = pd.DataFrame({"x":years, "y":ys})
        # plt.bar(x=df.x, height=df.y, alpha=0.7);
        # plt.gca().xaxis.set_major_formatter(FuncFormatter(lambda x, _: int(x)))
        # plt.scatter(x=dat_author.pub_year, y=dat_author.younger/dat_author.younger.sum())

        switchpoint = years[np.argmax(ys)]
        for year in years:
            idx = year < switchpoint
            out[(name, year)] = np.mean(np.where(idx, trace["e"], trace["l"]))
        
        dat['changing_rate'] = dat.apply(lambda row: out.get((row['name'], row['pub_year']), None), axis=1)

    dat.to_parquet(OUTPUT_DIR / 'training_data.parquet', index=False)

if __name__ == "__main__":
    main()





# plt.figure(figsize=(7, 4))
# ax=sns.scatterplot(x='pub_year', y='density', data=dat_author)
# plt.ylabel("Density younger coauthorships")
# plt.gca().xaxis.set_major_formatter(FuncFormatter(lambda x, _: int(x)))
# plt.xlabel("Year")

# nb_collabs = dat_author.younger.astype(int)
# years = dat_author.pub_year.tolist()

# sns.scatterplot(x=years, y=nb_collabs)
# plt.ylabel("# collab younger")
# plt.xlabel("Year")
# plt.suptitle("Aaron Clauset")
# plt.savefig("clauset_density.pdf")

# sns.histplot([np.random.poisson(np.random.gamma(3, 1)) for _ in range(1000)])

# stan_file = os.path.join('stan', 'change_point02.stan')
# model = CmdStanModel(stan_file=stan_file)


# data  = {
#     "T" : len(years),
#     "D" : nb_collabs.to_list()
# }


# fit = model.sample(data=data)
# fit_az = az.InferenceData(posterior=fit.draws_xr())
# post = fit_az.posterior
# trace = post.stack(draws=("chain", "draw"))

# az.plot_trace(fit_az, var_names=('e', 'l'))
# plt.tight_layout()

# fit.summary()

# ys = [np.exp(_[-1]) for _ in trace['lp'].to_numpy()] 
# ys /= np.sum(ys)
# df=pd.DataFrame({"x":years, "y":np.round(ys, 3)})
# sns.barplot(x='x', y='y', data=df[df.y > 1e-4], color='midnightblue', alpha=0.7);

# plt.figure(figsize=(7, 4))

# plt.plot(years, nb_collabs, ".", alpha=0.6)
# plt.ylabel("# younger coauthors", fontsize=16)
# plt.xlabel("Year", fontsize=16)
# plt.suptitle("Aaron Clauset", fontsize=16)

# year_s = df.loc[df.y.argmax(), 'x']
# plt.vlines(year_s, nb_collabs.min(), nb_collabs.max(), color="C1")
# average_disasters = np.zeros_like(nb_collabs, dtype="float")
# for i, year in enumerate(years):
#     idx = year < year_s
#     average_disasters[i] = np.mean(np.where(idx, trace["e"], trace["l"]))

# from  matplotlib.ticker import FuncFormatter

# plt.fill_betweenx(
#     y=[nb_collabs.min(), nb_collabs.max()],
#     x1=df[df.y > 1e-4].min()['x'],
#     x2=df[df.y > 1e-4].max()['x'],
#     alpha=0.9,
#     color="C1",
# )
# plt.fill_betweenx(
#     y=[nb_collabs.min(), nb_collabs.max()],
#     x1=df[df.y > 1e-10].min()['x'],
#     x2=df[df.y > 1e-10].max()['x'],
#     alpha=0.4,
#     color="C1",
# )
# plt.plot(years, average_disasters, "k--", lw=2);

# plt.gca().xaxis.set_major_formatter(FuncFormatter(lambda x, _: int(x)))
# plt.savefig("clauset_switchpoint.pdf")
