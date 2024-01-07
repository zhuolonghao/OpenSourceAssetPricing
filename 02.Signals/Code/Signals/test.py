import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Function to plot a hexagon
def plot_hexagon(data):
    angles = np.linspace(0, 2*np.pi, 7)  # Angles for hexagon vertices
    x = np.sin(angles)
    y = np.cos(angles)

    plt.figure(figsize=(6, 6))
    plt.title('Hexagon Plot')

    # Plotting hexagon vertices
    plt.plot(x, y, 'ko-')

    # Plotting data on hexagon vertices
    for i, (xi, yi) in enumerate(zip(x, y)):
        if i < 6:
            plt.text(xi, yi, f'{data[i]}', ha='center', va='center', fontsize=8)

    plt.axis('equal')
    plt.grid(True)
    plt.show()

# Example data for each dimension (replace this with your own data)
dimension_data = ['Momentum', "Profitability", "Reversal", "13F", "Seasonality", "Valuation"]

plot_hexagon(dimension_data)




import matplotlib.pyplot as plt
import numpy as np

# Define the coordinates of the hexagon vertices
hexagon_vertices = np.array([
    [0, 1],     # Top
    [np.sqrt(3)/2, 0.5],   # Upper right
    [np.sqrt(3)/2, -0.5],  # Lower right
    [0, -1],    # Bottom
    [-np.sqrt(3)/2, -0.5],  # Lower left
    [-np.sqrt(3)/2, 0.5]    # Upper left
])

# Add the first point to the end to close the shape
hexagon_vertices = np.vstack((hexagon_vertices, hexagon_vertices[0]))

# Plotting the hexagon
plt.figure(figsize=(6, 6))
plt.plot(hexagon_vertices[:, 0], hexagon_vertices[:, 1], color='black')  # Plot the hexagon shape
plt.fill(hexagon_vertices[:, 0], hexagon_vertices[:, 1], color='skyblue', alpha=0.5)  # Fill the hexagon

# Add labels to each vertex (optional)
for i, vertex in enumerate(hexagon_vertices[:-1]):
    plt.text(vertex[0], vertex[1], f'{i+1}', ha='center', va='center')

plt.axis('equal')
plt.title('Hexagon Shape')
plt.xlabel('X-axis')
plt.ylabel('Y-axis')
plt.grid(True)
plt.show()


########################################################
########################################################


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_excel(rf"C:\Users\longh\Desktop\OpenSourceAP_Python\02.Signals\rankings_202311.xlsx",
                   sheet_name='non_micro_tickers')
keys = ['ticker', 'sector',
       'momentum', 'profitability', 'reversal',  '13F', 'seasonality', 'valuation']
x = df[keys].iloc[3]

dimension = ['Momentum', "Profitability",  "13F", "Seasonality", "Valuation"]

# Define circle parameters
center = (0, 0)  # Center of the circle
radius = 1.0  # Radius of the circle
# Create a circle
circle = plt.Circle(center, radius, color='skyblue', fill=False)


# Create a figure and axis
fig, ax = plt.subplots(figsize=(4, 4))
# Add the circle to the plot
ax.add_patch(circle)
# Calculate the positions of hexagon's vertices
hexagon_vertices = np.array([
    [0, 1],     # Top
    [np.sqrt(3)/2, 0.5],   # Upper right
    [np.sqrt(3)/2, -0.5],  # Lower right
    [0, -1],    # Bottom
    [-np.sqrt(3)/2, -0.5],  # Lower left
    [-np.sqrt(3)/2, 0.5]    # Upper left
])
hexagon_ticker = hexagon_vertices.copy()

# Plotting the hexagon
dimension = ['Momentum', "Profitability", "Reversal", "13F", "Seasonality", "Valuation"]
for i, vertex in enumerate(hexagon_vertices):
    print(x[i+2]/6)
    hexagon_ticker[i] = hexagon_ticker[i]
    plt.text(vertex[0], vertex[1], f'{dimension[i]}', ha='center', va='center', weight='heavy', fontsize=8)
plt.text(0, 0, '+', ha='center', va='center', weight='normal', fontsize=16)
ax.fill(hexagon_ticker[:, 0], hexagon_ticker[:, 1], color='salmon', alpha=0.5)  # Fill the hexagon

# Show the plot
ax.axis('off')
ax.set_aspect('equal')
ticker = 'WSM'
sector = 'Consumer Discretionary'
ax.set_title(f"{ticker} --- {sector}", fontsize=14)
plt.tight_layout()
plt.grid(False)
plt.show()