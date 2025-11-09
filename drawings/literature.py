import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Polygon, FancyArrowPatch
import numpy as np

# Create figure with larger size
fig, ax = plt.subplots(1, 1, figsize=(14, 16))
ax.set_xlim(0, 10)
ax.set_ylim(0, 12)
ax.axis('off')

# Colors - keeping colors for boxes but all text will be black
color_search = '#3498db'
color_criteria = '#f39c12'
color_snowball = '#e74c3c'
color_final = '#2ecc71'
text_color = 'black'  # All text will be black

# Title - much larger
ax.text(5, 11.3, 'Literature Review Funnel', 
        ha='center', fontsize=28, fontweight='bold', color=text_color)

# Top section - Initial Search (widest part)
top_y = 9.8
top_width = 7.5
top_height = 1.2
top_box = mpatches.FancyBboxPatch((5 - top_width/2, top_y - 0.6), top_width, top_height,
                                   boxstyle="round,pad=0.1",
                                   edgecolor=color_search,
                                   facecolor=color_search,
                                   alpha=0.3,
                                   linewidth=2)
ax.add_patch(top_box)
ax.text(5, top_y + 0.3, 'Initial Keyword Search (Scopus)', 
        ha='center', fontsize=18, fontweight='bold', color=text_color)
ax.text(5, top_y - 0.05, '"asynchronous I/O" AND database OR io_uring OR', 
        ha='center', fontsize=14, style='italic', color=text_color)
ax.text(5, top_y - 0.35, 'PostgreSQL AND "database performance" AND ("I/O workers" OR "background writer")', 
        ha='center', fontsize=14, style='italic', color=text_color)

# Calculate top box bottom edge
top_bottom = top_y - 0.6

# Screening Criteria section
criteria_y = 7.3
criteria_width = 6.0
criteria_height = 1.6
criteria_box = mpatches.FancyBboxPatch((5 - criteria_width/2, criteria_y - 0.8), 
                                       criteria_width, criteria_height,
                                       boxstyle="round,pad=0.1",
                                       edgecolor=color_criteria,
                                       facecolor=color_criteria,
                                       alpha=0.3,
                                       linewidth=2)
ax.add_patch(criteria_box)
ax.text(5, criteria_y + 0.5, 'Screening for Relevance', 
        ha='center', fontsize=18, fontweight='bold', color=text_color)
ax.text(5, criteria_y - 0.05, 'Useful to our investigation?', 
        ha='center', fontsize=16, style='italic', color=text_color)
ax.text(5, criteria_y - 0.35, 'ACM Digital Library | IEEE Xplore', 
        ha='center', fontsize=14, style='italic', color=text_color)

# Calculate criteria box top and bottom edges
criteria_top = criteria_y + 0.8
criteria_bottom = criteria_y - 0.8

# Arrow from top box to criteria box
arrow1 = FancyArrowPatch((5, top_bottom), (5, criteria_top),
                        arrowstyle='->', mutation_scale=15,
                        linewidth=2, color='gray', alpha=0.7)
ax.add_patch(arrow1)

# Snowballing section
snowball_y = 5.0
snowball_width = 4.8
snowball_height = 1.1
snowball_box = mpatches.FancyBboxPatch((5 - snowball_width/2, snowball_y - 0.55), 
                                       snowball_width, snowball_height,
                                       boxstyle="round,pad=0.1",
                                       edgecolor=color_snowball,
                                       facecolor=color_snowball,
                                       alpha=0.3,
                                       linewidth=2)
ax.add_patch(snowball_box)
ax.text(5, snowball_y + 0.3, 'Snowballing', 
        ha='center', fontsize=18, fontweight='bold', color=text_color)
ax.text(5, snowball_y - 0.15, 'Backward & Forward', 
        ha='center', fontsize=16, style='italic', color=text_color)

# Calculate snowball box top and bottom edges
snowball_top = snowball_y + 0.55
snowball_bottom = snowball_y - 0.55

# Arrow from criteria box to snowball box
arrow2 = FancyArrowPatch((5, criteria_bottom), (5, snowball_top),
                        arrowstyle='->', mutation_scale=15,
                        linewidth=2, color='gray', alpha=0.7)
ax.add_patch(arrow2)

# Final output - wider bottom box
final_y = 2.2
final_width = 3.2
final_height = 1.8

# Final result box
final_box = mpatches.FancyBboxPatch((5 - final_width/2, final_y - 0.7), 
                                    final_width, final_height + 0.5,
                                    boxstyle="round,pad=0.1",
                                    edgecolor=color_final,
                                    facecolor=color_final,
                                    alpha=0.4,
                                    linewidth=3)
ax.add_patch(final_box)
ax.text(5, final_y + 1.0, 'Final Selection', 
        ha='center', fontsize=20, fontweight='bold', color=text_color)
ax.text(5, final_y + 0.3, '14 Studies', 
        ha='center', fontsize=26, fontweight='bold', color=text_color)
ax.text(5, final_y - 0.15, 'Sync vs Async I/O Performance', 
        ha='center', fontsize=14, style='italic', color=text_color)
ax.text(5, final_y - 0.45, 'or interesting in other way to our investigation',
        ha='center', fontsize=12, style='italic', color=text_color)

# Calculate final box top edge
final_top = final_y + 1.0 + 0.5

# Arrow from snowball box to final box
arrow3 = FancyArrowPatch((5, snowball_bottom), (5, final_top),
                        arrowstyle='->', mutation_scale=15,
                        linewidth=2, color='gray', alpha=0.7)
ax.add_patch(arrow3)

plt.tight_layout()

# Save as PDF for LaTeX
plt.savefig('literature_review_funnel.pdf', dpi=300, bbox_inches='tight', 
            facecolor='white', edgecolor='none', format='pdf')

# Also save as PNG as backup
plt.savefig('literature_review_funnel.png', dpi=300, bbox_inches='tight', 
            facecolor='white', edgecolor='none')

print("Figures saved as 'literature_review_funnel.pdf' and 'literature_review_funnel.png'")

plt.show()