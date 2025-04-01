import json
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
DWH_PATH = "output/dwh"
DASHBOARD_PATH = "output/dashboard"

def setup_directories():
    """Create output directories"""
    os.makedirs(DASHBOARD_PATH, exist_ok=True)
    logger.info(f"Created directory: {DASHBOARD_PATH}")

def load_json_data(file_path):
    """Load JSON data from file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None

def generate_top_movies_html(top_movies):
    """Generate HTML for top movies chart"""
    if not top_movies:
        return ""
    
    labels = [movie['title'] for movie in top_movies]
    ratings = [movie['avg_rating'] for movie in top_movies]
    counts = [movie['num_ratings'] for movie in top_movies]
    
    # Reverse the data to show highest rated at the top
    labels.reverse()
    ratings.reverse()
    counts.reverse()
    
    return f"""
    <div class="chart-container">
        <h2>Top 20 Movies by Rating</h2>
        <canvas id="topMoviesChart"></canvas>
        <script>
            new Chart(document.getElementById('topMoviesChart'), {{
                type: 'horizontalBar',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        label: 'Average Rating',
                        data: {json.dumps(ratings)},
                        backgroundColor: 'rgba(54, 162, 235, 0.6)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        xAxes: [{{
                            ticks: {{
                                beginAtZero: true,
                                max: 5
                            }}
                        }}]
                    }},
                    tooltips: {{
                        callbacks: {{
                            afterLabel: function(tooltipItem, data) {{
                                return 'Number of ratings: ' + {json.dumps(counts)}[tooltipItem.index];
                            }}
                        }}
                    }}
                }}
            }});
        </script>
    </div>
    """

def generate_rating_distribution_html(rating_dist):
    """Generate HTML for rating distribution chart"""
    if not rating_dist:
        return ""
    
    # Ensure proper order of rating tiers
    tier_order = ["Low", "Medium", "High", "Top"]
    
    # Sort data by tier order
    sorted_data = []
    for tier in tier_order:
        for item in rating_dist:
            if item.get('tier') == tier:
                sorted_data.append(item)
                break
    
    if not sorted_data:
        sorted_data = rating_dist
    
    labels = [item['tier'] for item in sorted_data]
    counts = [item['count'] for item in sorted_data]
    
    return f"""
    <div class="chart-container">
        <h2>Movie Rating Distribution</h2>
        <canvas id="ratingDistChart"></canvas>
        <script>
            new Chart(document.getElementById('ratingDistChart'), {{
                type: 'pie',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        data: {json.dumps(counts)},
                        backgroundColor: [
                            'rgba(255, 99, 132, 0.6)',
                            'rgba(255, 206, 86, 0.6)',
                            'rgba(54, 162, 235, 0.6)',
                            'rgba(75, 192, 192, 0.6)'
                        ],
                        borderColor: [
                            'rgba(255, 99, 132, 1)',
                            'rgba(255, 206, 86, 1)',
                            'rgba(54, 162, 235, 1)',
                            'rgba(75, 192, 192, 1)'
                        ],
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    legend: {{
                        position: 'right'
                    }},
                    tooltips: {{
                        callbacks: {{
                            label: function(tooltipItem, data) {{
                                var dataset = data.datasets[tooltipItem.datasetIndex];
                                var total = dataset.data.reduce(function(previousValue, currentValue) {{
                                    return previousValue + currentValue;
                                }});
                                var currentValue = dataset.data[tooltipItem.index];
                                var percentage = Math.round((currentValue/total) * 100);
                                return data.labels[tooltipItem.index] + ': ' + currentValue + ' (' + percentage + '%)';
                            }}
                        }}
                    }}
                }}
            }});
        </script>
    </div>
    """

def generate_movies_by_year_html(year_data):
    """Generate HTML for movies by year chart"""
    if not year_data:
        return ""
    
    # Filter to more recent years to keep the chart readable (last 50 years)
    current_year = datetime.now().year
    start_year = current_year - 50
    filtered_data = [item for item in year_data if item.get('year', 0) >= start_year]
    
    years = [item['year'] for item in filtered_data]
    movie_counts = [item['movie_count'] for item in filtered_data]
    avg_ratings = [item['avg_rating'] for item in filtered_data]
    
    return f"""
    <div class="chart-container">
        <h2>Movies by Year (Last 50 Years)</h2>
        <canvas id="moviesByYearChart"></canvas>
        <script>
            new Chart(document.getElementById('moviesByYearChart'), {{
                type: 'line',
                data: {{
                    labels: {json.dumps(years)},
                    datasets: [
                        {{
                            label: 'Number of Movies',
                            data: {json.dumps(movie_counts)},
                            backgroundColor: 'rgba(255, 99, 132, 0.2)',
                            borderColor: 'rgba(255, 99, 132, 1)',
                            borderWidth: 1,
                            yAxisID: 'y-axis-1'
                        }},
                        {{
                            label: 'Average Rating',
                            data: {json.dumps(avg_ratings)},
                            backgroundColor: 'rgba(54, 162, 235, 0.2)',
                            borderColor: 'rgba(54, 162, 235, 1)',
                            borderWidth: 1,
                            yAxisID: 'y-axis-2'
                        }}
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {{
                        yAxes: [
                            {{
                                type: 'linear',
                                display: true,
                                position: 'left',
                                id: 'y-axis-1',
                                scaleLabel: {{
                                    display: true,
                                    labelString: 'Number of Movies'
                                }}
                            }},
                            {{
                                type: 'linear',
                                display: true,
                                position: 'right',
                                id: 'y-axis-2',
                                gridLines: {{
                                    drawOnChartArea: false
                                }},
                                ticks: {{
                                    min: 0,
                                    max: 5
                                }},
                                scaleLabel: {{
                                    display: true,
                                    labelString: 'Average Rating'
                                }}
                            }}
                        ]
                    }}
                }}
            }});
        </script>
    </div>
    """

def generate_dashboard_html():
    """Generate HTML dashboard page"""
    # Load data
    top_movies = load_json_data(f"{DWH_PATH}/top_movies_by_rating.json")
    rating_dist = load_json_data(f"{DWH_PATH}/rating_distribution.json")
    year_data = load_json_data(f"{DWH_PATH}/movies_by_year.json")
    
    # Generate HTML components
    top_movies_html = generate_top_movies_html(top_movies)
    rating_dist_html = generate_rating_distribution_html(rating_dist)
    movies_by_year_html = generate_movies_by_year_html(year_data)
    
    # Get current timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create full HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MovieLens Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@2.9.4"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
            border-radius: 5px;
        }}
        header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        h1 {{
            color: #333;
        }}
        .dashboard {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            grid-gap: 20px;
        }}
        .chart-container {{
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            box-shadow: 0 0 5px rgba(0, 0, 0, 0.1);
            height: 400px;
        }}
        .full-width {{
            grid-column: 1 / -1;
        }}
        footer {{
            margin-top: 30px;
            text-align: center;
            color: #666;
            font-size: 0.8em;
        }}
        @media (max-width: 768px) {{
            .dashboard {{
                grid-template-columns: 1fr;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>MovieLens 20M Dataset Dashboard</h1>
            <p>Visualization of movie ratings and analytics</p>
        </header>
        
        <div class="dashboard">
            {rating_dist_html}
            {top_movies_html}
            <div class="full-width">
                {movies_by_year_html}
            </div>
        </div>
        
        <footer>
            <p>Generated on {timestamp} | MovieLens 20M Pipeline Project</p>
        </footer>
    </div>
</body>
</html>
    """
    
    # Write to file
    dashboard_file = f"{DASHBOARD_PATH}/index.html"
    with open(dashboard_file, 'w') as f:
        f.write(html)
    
    logger.info(f"Generated dashboard at: {dashboard_file}")
    return dashboard_file

def main():
    """Main function to generate dashboard"""
    try:
        logger.info("Starting dashboard generation")
        
        # Setup directories
        setup_directories()
        
        # Generate dashboard
        dashboard_file = generate_dashboard_html()
        
        logger.info(f"Dashboard generation completed: {dashboard_file}")
        logger.info(f"Open the dashboard in a web browser: file://{os.path.abspath(dashboard_file)}")
    
    except Exception as e:
        logger.error(f"Error generating dashboard: {e}")

if __name__ == "__main__":
    main()

