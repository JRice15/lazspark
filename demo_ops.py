import stages

from utils import *
import matplotlib.pyplot as plt
import seaborn


def plot_points(pts, title=None):
    df = pd.DataFrame(pts, columns=["x", "y", "z"])
    plt.xlim(-5, 5)
    plt.ylim(-5, 5)
    seaborn.scatterplot(
        data=df,
        x="x", y="y", hue="z",
    )
    if title:
        plt.title(title)
    plt.show()


pts = np.random.randn(1000, 3)


x, _ = stages.FakeReader(points_per_chunk=100)(pts)
x = stages.Decimate(2)(x)
out1 = stages.Collect()(x)

x = stages.Filter(min_x=-1, max_y=2)(x)
out2 = stages.Collect()(x)

x = stages.Translate(-1, 1, 0.5)(x)
out3 = stages.Collect()(x)

x = stages.Scale(2, 1, 3)(x)
out4 = stages.Collect()(x)

plot_points(pts, title="original")
plot_points(out1, title="decimated")
plot_points(out2, title="filtered")
plot_points(out3, title="translated")
plot_points(out4, title="scaled")

