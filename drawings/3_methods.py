import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Rectangle

# Set style for academic paper with higher quality
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['font.size'] = 10
plt.rcParams['axes.linewidth'] = 1.5
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 600  # Very high DPI for publication

fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12), dpi=300)

# Colors for consistent styling
cpu_color = '#3498db'
io_color = '#e74c3c'
bg_worker_color = '#9b59b6'
completion_color = '#2ecc71'
text_color = '#2c3e50'

def draw_timeline(ax, title, blocks, y_positions, thread_labels=None, method_specific=None):
    """Draw a timeline with CPU and I/O blocks for different threads"""
    ax.set_xlim(0, 180)
    ax.set_ylim(0, 1)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=35, color=text_color)
    
    # Draw timelines for each thread
    for i, y_pos in enumerate(y_positions):
        ax.axhline(y=y_pos, color='#7f8c8d', linewidth=2, alpha=0.7)
        if thread_labels:
            ax.text(-18, y_pos, thread_labels[i], ha='right', va='center', 
                    fontweight='bold', fontsize=11, color=text_color)
    
    # Track label positions to avoid overlap
    label_positions = []
    
    # Draw blocks
    for block in blocks:
        if block['type'] == 'cpu':
            color = cpu_color
            edgecolor = '#2980b9'
        elif block['type'] == 'io':
            color = io_color
            edgecolor = '#c0392b'
        elif block['type'] == 'bg_worker':
            color = bg_worker_color
            edgecolor = '#8e44ad'
        elif block['type'] == 'completion':
            color = completion_color
            edgecolor = '#27ae60'
        else:
            color = '#bdc3c7'
            edgecolor = '#95a5a6'
        
        rect = Rectangle((block['start'], block['y']-0.12), 
                        block['duration'], 0.24,
                        facecolor=color, edgecolor=edgecolor, linewidth=1.5,
                        alpha=0.9)
        ax.add_patch(rect)
        
        # Calculate label position with smart placement to avoid overlap
        label_x = block['start'] + block['duration']/2
        base_label_y = block['y'] + 0.18
        
        # Check for nearby labels and adjust position if needed
        label_y = base_label_y
        for prev_x, prev_y in label_positions:
            if abs(label_x - prev_x) < 15 and abs(label_y - prev_y) < 0.15:
                label_y = prev_y + 0.08  # Stack labels vertically
        
        label_positions.append((label_x, label_y))
        
        ax.text(label_x, label_y, 
                block['label'], ha='center', va='center', 
                fontweight='bold', color=text_color, fontsize=10,
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', 
                         edgecolor='gray', alpha=0.95, linewidth=0.8))
    
    # Draw method-specific elements
    if method_specific:
        for element in method_specific:
            if element['type'] == 'arrow':
                ax.arrow(element['x'], element['y'], element['dx'], element['dy'],
                        head_width=0.025, head_length=3, fc=element['color'], 
                        ec=element['color'], linewidth=1.5, alpha=0.8)
            elif element['type'] == 'bracket':
                x, y, width = element['x'], element['y'], element['width']
                if element['orientation'] == 'down':
                    ax.plot([x, x], [y, y-0.06], color=element['color'], linewidth=2.5)
                    ax.plot([x+width, x+width], [y, y-0.06], color=element['color'], linewidth=2.5)
                    ax.plot([x, x+width], [y-0.06, y-0.06], color=element['color'], linewidth=2.5)

    # Remove axes
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)

# 1. SYNCHRONOUS I/O
sync_blocks = [
    {'type': 'cpu', 'start': 0, 'duration': 15, 'y': 0.7, 'label': 'CPU'},
    {'type': 'io', 'start': 15, 'duration': 35, 'y': 0.7, 'label': 'I/O 1'},
    {'type': 'cpu', 'start': 50, 'duration': 15, 'y': 0.7, 'label': 'CPU'},
    {'type': 'io', 'start': 65, 'duration': 35, 'y': 0.7, 'label': 'I/O 2'},
    {'type': 'cpu', 'start': 100, 'duration': 15, 'y': 0.7, 'label': 'CPU'},
    {'type': 'io', 'start': 115, 'duration': 35, 'y': 0.7, 'label': 'I/O 3'},
    {'type': 'cpu', 'start': 150, 'duration': 15, 'y': 0.7, 'label': 'CPU'}
]
draw_timeline(ax1, "1. Synchronous I/O", sync_blocks, [0.7], ["Main Thread"])

# 2. BACKGROUND WORKERS (with 5 workers total)
bg_blocks = [
    # Main thread
    {'type': 'cpu', 'start': 0, 'duration': 8, 'y': 0.85, 'label': 'Sub 1'},
    {'type': 'cpu', 'start': 20, 'duration': 8, 'y': 0.85, 'label': 'Sub 2'},
    {'type': 'cpu', 'start': 40, 'duration': 8, 'y': 0.85, 'label': 'Sub 3'},
    {'type': 'cpu', 'start': 60, 'duration': 8, 'y': 0.85, 'label': 'Sub 4'},
    {'type': 'cpu', 'start': 80, 'duration': 8, 'y': 0.85, 'label': 'Sub 5'},
    {'type': 'cpu', 'start': 135, 'duration': 30, 'y': 0.85, 'label': 'Process Results'},
    
    # Background workers - Worker 1
    {'type': 'bg_worker', 'start': 8, 'duration': 25, 'y': 0.65, 'label': 'I/O 1'},
    
    # Worker 2
    {'type': 'bg_worker', 'start': 28, 'duration': 25, 'y': 0.55, 'label': 'I/O 2'},
    
    # Worker 3
    {'type': 'bg_worker', 'start': 48, 'duration': 25, 'y': 0.45, 'label': 'I/O 3'},
    
    # Worker 4
    {'type': 'bg_worker', 'start': 68, 'duration': 25, 'y': 0.35, 'label': 'I/O 4'},
    
    # Worker 5
    {'type': 'bg_worker', 'start': 88, 'duration': 25, 'y': 0.25, 'label': 'I/O 5'},
    
    # Completion notifications
    {'type': 'completion', 'start': 33, 'duration': 4, 'y': 0.85, 'label': 'D1'},
    {'type': 'completion', 'start': 53, 'duration': 4, 'y': 0.85, 'label': 'D2'},
    {'type': 'completion', 'start': 73, 'duration': 4, 'y': 0.85, 'label': 'D3'},
    {'type': 'completion', 'start': 93, 'duration': 4, 'y': 0.85, 'label': 'D4'},
    {'type': 'completion', 'start': 113, 'duration': 4, 'y': 0.85, 'label': 'D5'}
]

bg_elements = [
    # Submission arrows
    {'type': 'arrow', 'x': 8, 'y': 0.78, 'dx': 0, 'dy': -0.12, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 28, 'y': 0.78, 'dx': 0, 'dy': -0.22, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 48, 'y': 0.78, 'dx': 0, 'dy': -0.32, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 68, 'y': 0.78, 'dx': 0, 'dy': -0.42, 'color': bg_worker_color},
    {'type': 'arrow', 'x': 88, 'y': 0.78, 'dx': 0, 'dy': -0.52, 'color': bg_worker_color},
    # Completion arrows
    {'type': 'arrow', 'x': 33, 'y': 0.67, 'dx': 0, 'dy': 0.12, 'color': completion_color},
    {'type': 'arrow', 'x': 53, 'y': 0.57, 'dx': 0, 'dy': 0.22, 'color': completion_color},
    {'type': 'arrow', 'x': 73, 'y': 0.47, 'dx': 0, 'dy': 0.32, 'color': completion_color},
    {'type': 'arrow', 'x': 93, 'y': 0.37, 'dx': 0, 'dy': 0.42, 'color': completion_color},
    {'type': 'arrow', 'x': 113, 'y': 0.27, 'dx': 0, 'dy': 0.52, 'color': completion_color}
]

draw_timeline(ax2, "2. Background Workers (5 Workers)", bg_blocks, 
              [0.85, 0.65, 0.55, 0.45, 0.35, 0.25], 
              ["Main Thread", "Worker 1", "Worker 2", "Worker 3", "Worker 4", "Worker 5"], 
              bg_elements)

# 3. IO_URING
uring_blocks = [
    # Main thread - continuous processing
    {'type': 'cpu', 'start': 0, 'duration': 140, 'y': 0.8, 'label': 'Continuous CPU Processing'},
    
    # Submission queue
    {'type': 'cpu', 'start': 5, 'duration': 5, 'y': 0.6, 'label': 'SQE 1'},
    {'type': 'cpu', 'start': 25, 'duration': 5, 'y': 0.6, 'label': 'SQE 2'},
    {'type': 'cpu', 'start': 45, 'duration': 5, 'y': 0.6, 'label': 'SQE 3'},
    
    # Completion queue
    {'type': 'completion', 'start': 30, 'duration': 5, 'y': 0.4, 'label': 'CQE 1'},
    {'type': 'completion', 'start': 50, 'duration': 5, 'y': 0.4, 'label': 'CQE 2'},
    {'type': 'completion', 'start': 70, 'duration': 5, 'y': 0.4, 'label': 'CQE 3'},
    
    # Kernel I/O (happening asynchronously)
    {'type': 'io', 'start': 10, 'duration': 20, 'y': 0.2, 'label': 'I/O 1'},
    {'type': 'io', 'start': 30, 'duration': 20, 'y': 0.2, 'label': 'I/O 2'},
    {'type': 'io', 'start': 50, 'duration': 20, 'y': 0.2, 'label': 'I/O 3'}
]

uring_elements = [
    # Submission arrows
    {'type': 'arrow', 'x': 10, 'y': 0.56, 'dx': 0, 'dy': -0.15, 'color': cpu_color},
    {'type': 'arrow', 'x': 30, 'y': 0.56, 'dx': 0, 'dy': -0.15, 'color': cpu_color},
    {'type': 'arrow', 'x': 50, 'y': 0.56, 'dx': 0, 'dy': -0.15, 'color': cpu_color},
    # Completion arrows
    {'type': 'arrow', 'x': 30, 'y': 0.44, 'dx': 0, 'dy': 0.15, 'color': completion_color},
    {'type': 'arrow', 'x': 50, 'y': 0.44, 'dx': 0, 'dy': 0.15, 'color': completion_color},
    {'type': 'arrow', 'x': 70, 'y': 0.44, 'dx': 0, 'dy': 0.15, 'color': completion_color},
    # Kernel processing bracket
    {'type': 'bracket', 'x': 0, 'y': 0.1, 'width': 180, 'orientation': 'down', 'color': io_color}
]

draw_timeline(ax3, "3. io_uring", uring_blocks, [0.8, 0.6, 0.4, 0.2], 
              ["Main Thread", "Submission Queue", "Completion Queue", "Kernel I/O"], 
              uring_elements)

# Add kernel label for io_uring
ax3.text(90, 0.1, "Kernel Space", ha='center', va='center', 
         fontweight='bold', fontsize=11, color=text_color,
         bbox=dict(boxstyle="round,pad=0.4", facecolor='white', 
                  edgecolor=io_color, linewidth=2))

# Add legend with better styling
legend_elements = [
    Rectangle((0, 0), 1, 1, facecolor=cpu_color, edgecolor='#2980b9', 
              linewidth=1.5, label='CPU Processing'),
    Rectangle((0, 0), 1, 1, facecolor=io_color, edgecolor='#c0392b', 
              linewidth=1.5, label='I/O Operation'),
    Rectangle((0, 0), 1, 1, facecolor=bg_worker_color, edgecolor='#8e44ad', 
              linewidth=1.5, label='Background Worker'),
    Rectangle((0, 0), 1, 1, facecolor=completion_color, edgecolor='#27ae60', 
              linewidth=1.5, label='Completion')
]

fig.legend(handles=legend_elements, 
           loc='lower center', 
           bbox_to_anchor=(0.5, 0.01),
           ncol=4, 
           frameon=True,
           fontsize=12,
           fancybox=True,
           shadow=True,
           framealpha=1,
           edgecolor='gray',
           borderpad=1)

plt.tight_layout()
plt.subplots_adjust(bottom=0.1, hspace=0.9)

# Save as publication-quality PNG and vector format
plt.savefig('three_methods.png', 
            dpi=600,  # High DPI for crisp printing
            bbox_inches='tight', 
            facecolor='white',
            edgecolor='none',
            pad_inches=0.1,
            format='png')

# Also save as vector format (PDF) for ultimate quality
plt.savefig('three_methods.pdf', 
            bbox_inches='tight', 
            facecolor='white',
            edgecolor='none',
            pad_inches=0.1,
            format='pdf')

plt.show()

print("High-quality diagrams saved as:")
print("  - 'three_methods.png' (600 DPI raster)")
print("  - 'three_methods.pdf' (vector format - best for papers)")
print("\nFor LaTeX papers, use the PDF version for perfect scaling!")