import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# Sample DataFrame for the table
data = {'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Age': [25, 30, 35, 40],
        'Salary': [50000, 60000, 75000, 90000]}

df = pd.DataFrame(data)

# Sample data for the figure
x_values = [1, 2, 3, 4]
y_values = [10, 15, 7, 12]
z_values = [0,0,0,0]

# Create a PDF file to save the table and figure
pdf_file_path = 'output.pdf'

with PdfPages(pdf_file_path, keep_empty=False) as pdf:
    # Set letter size (8.5 x 11 inches) for each page
    fig, axes  = plt.subplots(figsize=(8.5, 11), nrows=3, ncols=1)


    # Creating a table from the DataFrame
    axes[0].axis('off')
    table_title = "Sample Table"

    table = axes[0].table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    axes[1].set_title('Sample Table')
    axes[0].set_title(table_title, fontsize=12)
    table.auto_set_font_size(True)
    table.auto_set_column_width(col=list(range(len(df.columns))))
    #table.set_fontsize(10)
    #table.scale(2, 2)  # Adjust the table size

    # Plotting the figure
    axes[1].plot(x_values, y_values, marker='o', linestyle='-', color='b', label='Series 1')
    axes[1].plot(x_values, z_values, marker='s', linestyle='--', color='r', label='Series 2')
    axes[1].legend(loc='best')
    axes[1].set_title('Sample Figure')
    axes[1].set_xlabel('X-axis')
    axes[1].set_ylabel('Y-axis')

    # Plotting the figure
    axes[2].plot(x_values, y_values, marker='o', linestyle='-', color='b')
    axes[2].plot(x_values, z_values, marker='s', linestyle='--', color='r', label='Series 3')
    axes[2].legend(loc='best')
    axes[2].set_title('Sample Figure2')
    axes[2].set_xlabel('X-axis')
    axes[2].set_ylabel('Y-axis')
    # Save the combined figure and table to the PDF
    plt.suptitle(f'Title for Page 1', fontsize=14, fontweight='bold', x=0.01, ha='left')
    plt.tight_layout()
    pdf.savefig()
    plt.close()

    # second page
    fig2, axes2 = plt.subplots(figsize=(8.5, 11), nrows=3, ncols=1)
    # Plotting the figure
    axes2[0].plot(x_values, y_values, marker='o', linestyle='-', color='b')
    axes2[0].set_title('Sample Figure3')
    axes2[0].set_xlabel('X-axis')
    axes2[0].set_ylabel('Y-axis')
    # Save the combined figure and table to the PDF
    plt.suptitle(f'Title for Page 2', fontsize=14, fontweight='bold', x=0.01, ha='left')
    plt.tight_layout()
    pdf.savefig()
    plt.close()