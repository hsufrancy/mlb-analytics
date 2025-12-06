"""
Flask Web App for MLB Analytics - FIXED WITH SSH KEY
"""
from flask import Flask, render_template, jsonify, request
import subprocess
import os
import requests

app = Flask(__name__)

# Configuration - ADD SSH KEY PATH
CLUSTER_HOST = "ec2-34-230-47-10.compute-1.amazonaws.com"
CLUSTER_USER = "hadoop"
SSH_KEY_PATH = os.path.expanduser("~/Desktop/UChicago/Big Data/key")  
HBASE_API_URL = "http://ec2-34-230-47-10.compute-1.amazonaws.com:5001/api/hbase/live-predictions"

def query_hive(query):
    """Execute Hive query via SSH with custom key"""
    try:
        ssh_command = f'ssh -i "{SSH_KEY_PATH}" {CLUSTER_USER}@{CLUSTER_HOST} "hive --silent -e \\"{query}\\""'
        
        
        result = subprocess.run(
            ssh_command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        
        if result.returncode != 0:
            print(f"Command failed!")
            return []
        
        lines = result.stdout.strip().split('\n')
        data = []
        
        for line in lines:
            line = line.strip()
            if line and not any(x in line.lower() for x in ['warn', 'info', 'time taken', 'ok']):
                if '\t' in line:
                    data.append(line.split('\t'))
                else:
                    data.append([line])
        
        return data
        
    except Exception as e:
        print(f"Error querying Hive: {e}")
        import traceback
        traceback.print_exc()
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/historical')
def historical():
    return render_template('historical.html')

@app.route('/api/live-games')
def get_live_games():
    """
    Get live games from HBase via hbase_api on the cluster.
    Requires SSH tunnel: local 5001 -> cluster 5001.
    """
    try:
        resp = requests.get(HBASE_API_URL, timeout=5)
        data = resp.json()

        games = []
        for item in data:
            games.append({
                "game_id": item.get("game_id"),
                "home_team": item.get("home_team", "HOME"),
                "away_team": item.get("away_team", "AWAY"),
                "home_score": item.get("home_score", 0),
                "away_score": item.get("away_score", 0),
                "inning": item.get("inning", "N/A"),
                "home_win_prob": item.get("home_win_prob", 0.5),
                "away_win_prob": item.get("away_win_prob", 0.5),
                "status": "LIVE",
            })

        return jsonify(games)

    except Exception as e:
        print(f"Error fetching live games from HBase API: {e}")
        return jsonify([])

@app.route('/api/team-performance')
def get_team_performance():
    team = request.args.get('team', 'NYY')
    season = request.args.get('season', '2018')
    
    query = f"""
    SELECT team, season, wins, losses, win_pct,
           home_wins, home_losses, away_wins, away_losses,
           runs_scored, runs_allowed, run_differential
    FROM mlb_analytics.team_season_stats
    WHERE team = '{team}' AND season = {season}
    """
    
    results = query_hive(query)
    
    if results and len(results) > 0:
        data = results[0]
        return jsonify({
            'team': data[0],
            'season': int(data[1]),
            'wins': int(data[2]),
            'losses': int(data[3]),
            'win_pct': float(data[4]),
            'home_record': f"{data[5]}-{data[6]}",
            'away_record': f"{data[7]}-{data[8]}",
            'runs_scored': int(data[9]),
            'runs_allowed': int(data[10]),
            'run_differential': int(data[11])
        })
    
    return jsonify({'error': 'No data found'})

@app.route('/api/matchup-history')
def get_matchup_history():
    team1 = request.args.get('team1', 'NYY')
    team2 = request.args.get('team2', 'BOS')
    season = request.args.get('season', '2018')
    
    query = f"""
    SELECT team1, team2, season, total_games,
           team1_wins, team2_wins, avg_total_runs
    FROM mlb_analytics.matchup_stats
    WHERE ((team1 = '{team1}' AND team2 = '{team2}')
       OR (team1 = '{team2}' AND team2 = '{team1}'))
      AND season = {season}
    LIMIT 1
    """
    
    results = query_hive(query)
    
    if results and len(results) > 0:
        data = results[0]
        return jsonify({
            'team1': data[0],
            'team2': data[1],
            'season': int(data[2]),
            'total_games': int(data[3]),
            'team1_wins': int(data[4]),
            'team2_wins': int(data[5]),
            'avg_runs': float(data[6])
        })
    
    return jsonify({'error': 'No matchup data found'})

@app.route('/api/league-trends')
def get_league_trends():
    query = """
    SELECT season, total_games, avg_runs_per_game,
           home_win_pct, close_game_pct
    FROM mlb_analytics.league_trends
    ORDER BY season
    """
    
    results = query_hive(query)
    
    trends = []
    for row in results:
        if len(row) >= 5:  # Changed from 6 to 5
            try:
                trends.append({
                    'season': int(row[0]),
                    'total_games': int(row[1]),
                    'avg_runs': float(row[2]),
                    'home_win_pct': float(row[3]),
                    'close_game_pct': float(row[4])
                    # Removed avg_attendance
                })
            except (ValueError, IndexError) as e:
                print(f"Skipping invalid row: {row}, error: {e}")
                continue
    
    return jsonify(trends)

@app.route('/api/teams')
def get_teams():
    """Get list of all teams"""
    query = """
    SELECT DISTINCT team
    FROM mlb_analytics.team_season_stats
    WHERE team NOT IN ('AL', 'NL')
    ORDER BY team
    """
    
    print(f"Fetching teams...")
    results = query_hive(query)
    print(f"Got {len(results)} results")
    
    teams = []
    for row in results:
        if row and len(row) > 0 and row[0]:
            teams.append(row[0])
    
    print(f"Final teams: {teams}")
    return jsonify(teams)

@app.route('/api/test-ssh')
def test_ssh():
    """Test SSH connection"""
    cmd = f'ssh -i "{SSH_KEY_PATH}" {CLUSTER_USER}@{CLUSTER_HOST} "echo SUCCESS"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
    
    return jsonify({
        'command': cmd,
        'returncode': result.returncode,
        'stdout': result.stdout,
        'stderr': result.stderr
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3025, debug=False)
