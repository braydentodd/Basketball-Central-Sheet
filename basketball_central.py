from datetime import datetime
import time
import gspread
import requests
import json
from google.oauth2.service_account import Credentials
from nba_api.stats.static import teams
from nba_api.stats.endpoints import commonteamroster, playerdashboardbygeneralsplits
from gspread.utils import rowcol_to_a1
import numpy as np
import matplotlib.colors as mcolors

# connects to the Google Sheet
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file(
    "basketball-central-449118-ea0521083234.json", scopes=scopes
)
client = gspread.authorize(creds)
sheet = client.open("Basketball_Central")
master_sheet = sheet.worksheet("NBA")

# list of all NBA team abbreviations (correlating to the team sheets in the Google Sheet)
team_sheets = [
    "ATL",
    "BOS",
    "BKN",
    "CHA",
    "CHI",
    "CLE",
    "DAL",
    "DEN",
    "DET",
    "GSW",
    "HOU",
    "IND",
    "LAC",
    "LAL",
    "MEM",
    "MIA",
    "MIL",
    "MIN",
    "NOP",
    "NYK",
    "OKC",
    "ORL",
    "PHI",
    "PHX",
    "POR",
    "SAC",
    "SAS",
    "TOR",
    "UTA",
    "WAS",
]

# columns containing per 100 stats on the team sheets
team_stat_columns = [
    "K",
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
]

# columns and rows for reference on the sheets
TEAM_ID_COLUMN = "AD"
TEAM_NOTES_COLUMN = "J"
REVERSED_TEAM_STATS_COLUMNS = ["X", "AC"]
PLAYER_DATA_START_ROW = 4
PLAYER_DATA_END_ROW = 26
PLAYER_INFO_START_COLUMN = "A"
PLAYER_INFO_END_COLUMN = "I"
PLAYER_INFO_START_COLUMN_NUM = 1
PLAYER_INFO_END_COLUMN_NUM = 9
PLAYER_STATS_START_COLUMN_NUM = 11
PLAYER_STATS_END_COLUMN_NUM = 30
PLAYER_STATS_START_COLUMN = "K"
MASTER_DATA_START_ROW = 1
MASTER_INFO_START_COLUMN_NUM = 1
MASTER_INFO_END_COLUMN_NUM = 10
MASTER_ID_COLUMN_NUM = 31

# columns containing per 100 stats on the master NBA sheet
master_stat_columns = [
    "L",
    "M",
    "N",
    "O",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
    "Y",
    "Z",
    "AA",
    "AB",
    "AC",
    "AD",
]

stats_collection = {
    category: [] for category in team_stat_columns
}  # stores stat values for percentile calculations

empty_rows = {}  # stores rows with no data for color coding
removed_players = {}


# fetches RAPM data for the current season
rapm_data = json.loads(
    requests.get("https://www.gameflowpbp.com/api/rapm_1?season=2025").json()
)
rapm_dict = {str(player["player_id"]): player for player in rapm_data}
most_games_played_player = max(rapm_dict.values(), key=lambda x: x["games_played"])
print(
    f"RAPM data fetched. Maximum games played of {most_games_played_player['games_played']} by {most_games_played_player['player_name']}."
)


# gets the current team roster and essential player info (name, J#, exp, birth date, ht, wt, etc)
def get_updated_team_roster(team_abbr):
    nba_teams = {team["abbreviation"]: team for team in teams.get_teams()}
    team_id = nba_teams[team_abbr]["id"]

    # pulls essential player data
    up_to_date_roster = (
        commonteamroster.CommonTeamRoster(team_id=team_id)
        .get_data_frames()[0]
        .astype({"PLAYER_ID": str})
        .set_index("PLAYER_ID")[
            ["PLAYER", "NUM", "EXP", "BIRTH_DATE", "HEIGHT", "WEIGHT"]
        ]
        .to_dict(orient="index")
    )

    return up_to_date_roster


# calculates current age (to 1 decimal place) from birth date
def calculate_age(birth_date):
    birth_date = datetime.strptime(birth_date, "%b %d, %Y")
    today = datetime.today()
    age = (today - birth_date).days / 365.25
    return round(age, 1)


# handles the removal of players no longer on a team
def clear_rows(team_sheet, rows_to_clear, master_player_ids):
    batch_player_removals = []
    master_player_updates = []

    for player in rows_to_clear.keys():
        # updates team status to master sheet
        for row_index, id in enumerate(master_player_ids, start=MASTER_DATA_START_ROW):
            if id == rows_to_clear[player][0]:
                master_row = row_index

                master_player_updates.append(
                    {
                        "range": f"D{master_row}",
                        "values": [["FA"]],  # team
                    },
                )
                break

        # clears player values on team sheet
        empty_player_info = [["" for col in range(PLAYER_INFO_END_COLUMN_NUM - 2)]]

        empty_player_stats = [
            [
                ""
                for col in range(
                    PLAYER_STATS_END_COLUMN_NUM - PLAYER_STATS_START_COLUMN_NUM
                )
            ]
        ]

        batch_player_removals.extend(
            [
                {
                    "range": f"{PLAYER_INFO_START_COLUMN}{player}",
                    "values": [[""]],
                },
                {
                    "range": f"{'B'}{player}",
                    "values": [["EMPTY"]],
                },
                {
                    "range": f"{rowcol_to_a1(player, PLAYER_INFO_START_COLUMN_NUM + 2)}:{rowcol_to_a1(player, PLAYER_INFO_END_COLUMN_NUM)}",
                    "values": empty_player_info,
                },
                {
                    "range": f"{TEAM_NOTES_COLUMN}{player}",
                    "values": [
                        [
                            "\n\n---------------------------------------------------------\n\nStrengths:\n - \n\nWeaknesses:\n - \n\nOther notes:\n - "
                        ]
                    ],
                },
                {
                    "range": f"{rowcol_to_a1(player, PLAYER_STATS_START_COLUMN_NUM)}:{rowcol_to_a1(player, PLAYER_STATS_END_COLUMN_NUM - 1)}",
                    "values": empty_player_stats,
                },
                {
                    "range": f"{TEAM_ID_COLUMN}{player}",
                    "values": [["-"]],
                },
            ]
        )

    if batch_player_removals:
        team_sheet.batch_update(batch_player_removals)
        reset_background_color(team_sheet, rows_to_clear)
        master_sheet.batch_update(master_player_updates)


# updates the team sheet
def update_team_sheet(team_abbr):
    update_player_data = []
    update_master_data = []
    master_reserved = []
    hardship_rows = []
    rows_to_clear = {}

    print(f"Updating {team_abbr} team sheet.")

    team_sheet = sheet.worksheet(team_abbr)
    up_to_date_roster = get_updated_team_roster(team_abbr)

    # pulls players_ids currently on the sheets
    existing_player_ids = team_sheet.col_values(PLAYER_STATS_END_COLUMN_NUM)[
        PLAYER_DATA_START_ROW - 1 :
    ]
    master_player_ids = master_sheet.col_values(MASTER_ID_COLUMN_NUM)

    # identifies the rows that need cleared
    for row_index, id in enumerate(existing_player_ids, start=PLAYER_DATA_START_ROW):
        if id not in up_to_date_roster:
            if team_abbr not in empty_rows:
                empty_rows[team_abbr] = []
            empty_rows[team_abbr].append(row_index)
            if id != "-":  # if the row contains a player
                rows_to_clear[row_index] = id
                player_data_row = team_sheet.row_values(row_index)[0:28]
                removed_players[id] = player_data_row
                print(f"Player ID #{id} removed from {team_abbr} team sheet.")
    if len(empty_rows[team_abbr]) > 4:
        hardship_rows = empty_rows[team_abbr][-4:]
    else:
        hardship_rows = empty_rows[team_abbr]

    if hardship_rows:
        for row in hardship_rows:
            update_player_data.append({"range": f"B{row}", "values": [["HARDSHIP"]]})

    if rows_to_clear:
        clear_rows(team_sheet, rows_to_clear, master_player_ids)
    if team_abbr in empty_rows:
        available_rows = empty_rows[team_abbr]

    for player_id, player_data in up_to_date_roster.items():
        # if player is already on the team sheet
        master_row = ""

        if player_id in existing_player_ids:
            row_index = existing_player_ids.index(player_id) + PLAYER_DATA_START_ROW
            print(f"Updating {player_data['PLAYER']}.")
        else:
            # if player is not on the team sheet
            row_index = available_rows.pop(0)
            print(f"Adding {player_data['PLAYER']} to {team_abbr}.")
            if player_id in removed_players:
                removed_players.remove(player_id)
            if player_id in master_player_ids:
                # copies personalized data to team sheet
                pos = master_sheet.cell(
                    master_player_ids.index(player_id) + MASTER_DATA_START_ROW,
                    MASTER_INFO_START_COLUMN_NUM + 3,
                ).value
                ws = master_sheet.cell(
                    master_player_ids.index(player_id) + MASTER_DATA_START_ROW,
                    MASTER_INFO_END_COLUMN_NUM - 1,
                ).value

                update_player_data.extend(
                    [
                        {"range": f"C{row_index}", "values": [[pos]]},
                        {"range": f"H{row_index}", "values": [[ws]]},
                    ]
                )
            else:
                update_player_data.extend(
                    [
                        {"range": f"C{row_index}", "values": [["?"]]},
                        {"range": f"H{row_index}", "values": [["?"]]},
                    ]
                )

        # finds player's row on the master sheet
        for master_row_index, id in enumerate(
            master_player_ids, start=MASTER_DATA_START_ROW
        ):
            if id == player_id:
                master_row = master_row_index
                print(
                    f"Updating {player_data['PLAYER']} on master sheet in row {master_row_index}."
                )
                break

        # if no row for player on master sheet, reserves first empty one for player
        if master_row == "":
            for master_row_index, id in enumerate(
                master_player_ids, start=MASTER_DATA_START_ROW
            ):
                if id == "-" and master_row_index not in master_reserved:
                    master_row = master_row_index
                    master_reserved.append(master_row_index)
                    print(
                        f"Adding {player_data['PLAYER']} on master sheet in row {master_row_index}."
                    )
                    break

        # reformats age and height data for better look
        age = calculate_age(player_data["BIRTH_DATE"])
        feet, inches = player_data["HEIGHT"].split("-")
        height = f"{feet}'{inches}\""

        # fetches per 100 possession player stats
        per_100_stats_data = (
            playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
                player_id=player_id,
                season="2024-25",
                per_mode_detailed="Per100Possessions",
            ).get_data_frames()[0]
        )

        update_player_data.extend(
            [
                {"range": f"B{row_index}", "values": [[player_data["PLAYER"]]]},
                {"range": f"D{row_index}", "values": [[player_data["NUM"]]]},
                {"range": f"E{row_index}", "values": [[player_data["EXP"]]]},
                {"range": f"F{row_index}", "values": [[age]]},
                {"range": f"G{row_index}", "values": [[height]]},
                {"range": f"I{row_index}", "values": [[player_data["WEIGHT"]]]},
                {"range": f"AD{row_index}", "values": [[player_id]]},
            ]
        )

        update_master_data.extend(
            [
                {"range": f"B{master_row}", "values": [[player_data["PLAYER"]]]},
                {"range": f"C{master_row}", "values": [[team_abbr]]},
                {"range": f"E{master_row}", "values": [[player_data["NUM"]]]},
                {"range": f"F{master_row}", "values": [[player_data["EXP"]]]},
                {"range": f"G{master_row}", "values": [[age]]},
                {"range": f"H{master_row}", "values": [[height]]},
                {"range": f"J{master_row}", "values": [[player_data["WEIGHT"]]]},
                {"range": f"AE{master_row}", "values": [[player_id]]},
            ]
        )

        # fetches advanced player stats
        adv_stats_data = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
            player_id=player_id, season="2024-25", measure_type_detailed="Advanced"
        ).get_data_frames()[0]

        # assigns values to player stat categories in correct formatting
        if not per_100_stats_data.empty:
            gm = int(per_100_stats_data["GP"].iloc[0])
            min = float(adv_stats_data["MIN"].iloc[0])

            if player_id in rapm_dict:
                orapm = round(float(rapm_dict[player_id]["off_rapm"]), 1)
                drapm = round(float(rapm_dict[player_id]["def_rapm"]), 1)
            else:
                orapm = 0
                drapm = 0
            pts = float(per_100_stats_data["PTS"].iloc[0])
            ts = float(adv_stats_data["TS_PCT"].iloc[0] * 100)
            fga = float(per_100_stats_data["FGA"].iloc[0])
            three_pa = float(per_100_stats_data["FG3A"].iloc[0])
            three_p_pct = float(per_100_stats_data["FG3_PCT"].iloc[0]) * 100
            two_pa = fga - three_pa
            if two_pa != 0:
                two_p_pct = round(
                    (
                        (
                            float(per_100_stats_data["FGM"].iloc[0])
                            - float(per_100_stats_data["FG3M"].iloc[0])
                        )
                        / two_pa
                    )
                    * 100,
                    1,
                )
            else:
                two_p_pct = 0
            fta = float(per_100_stats_data["FTA"].iloc[0])
            ft_pct = float(per_100_stats_data["FT_PCT"].iloc[0]) * 100
            ast = float(per_100_stats_data["AST"].iloc[0])
            tov = float(per_100_stats_data["TOV"].iloc[0])
            oreb = float(per_100_stats_data["OREB"].iloc[0])
            dreb = float(per_100_stats_data["DREB"].iloc[0])
            stl = float(per_100_stats_data["STL"].iloc[0])
            blk = float(per_100_stats_data["BLK"].iloc[0])
            fls = float(per_100_stats_data["PF"].iloc[0])

            stat_values = [
                gm,
                min,
                orapm,
                drapm,
                pts,
                ts,
                two_pa,
                two_p_pct,
                three_pa,
                three_p_pct,
                fta,
                ft_pct,
                ast,
                tov,
                oreb,
                dreb,
                stl,
                blk,
                fls,
            ]

            for i, col in enumerate(team_stat_columns):
                update_player_data.append(
                    {"range": f"{col}{row_index}", "values": [[stat_values[i]]]}
                )
                stats_collection[col].append((stat_values[i], min))
            for i, col in enumerate(master_stat_columns):
                update_master_data.append(
                    {"range": f"{col}{master_row}", "values": [[stat_values[i]]]}
                )

        # players with no recorded data
        else:
            for i, col in enumerate(team_stat_columns):
                update_player_data.append(
                    {"range": f"{col}{row_index}", "values": [[""]]}
                )
                if team_abbr not in empty_rows:
                    empty_rows[team_abbr] = []
                empty_rows[team_abbr].append(row_index)

            for i, col in enumerate(master_stat_columns):
                update_master_data.append(
                    {"range": f"{col}{master_row}", "values": [[""]]}
                )

    team_sheet.batch_update(update_player_data)
    print(f"{team_abbr} team sheet updated.")
    master_sheet.batch_update(update_master_data)
    print(f"{team_abbr} players updated to master sheet.")


def update_removed_players(removed_players):
    update_fa_data = []
    for player_id, player_data in removed_players.items():
        master_player_ids = master_sheet.col_values(MASTER_ID_COLUMN_NUM)
        for master_row_index, id in enumerate(
            master_player_ids, start=MASTER_DATA_START_ROW
        ):
            if id == player_id:
                master_row = master_row_index
                update_fa_data.extend(
                    [
                        {
                            "range": "A{master_row}:B{master_row}",
                            "values": [[player_data[0:2]]],
                        },
                        {"range": "C{master_row}", "values": [["FA"]]},
                        {
                            "range": "D{master_row}:AC{master_row}",
                            "values": [[player_data[2:29]]],
                        },
                        {"range": "AD{master_row}", "values": [[player_id]]},
                    ]
                )
                break

        print(f"Updating Free Agent {player_data[1]} on master sheet.")
    master_sheet.batch_update(update_fa_data)
    print("Free Agents updated to master sheet.")


def scrape_team_sheets(team_abbr):
    team_sheet = sheet.worksheet(team_abbr)
    stats_range = team_sheet.get_all_values(
        f"{PLAYER_STATS_START_COLUMN}{PLAYER_DATA_START_ROW}:{TEAM_ID_COLUMN}{PLAYER_DATA_END_ROW}"
    )

    for row_index, player in enumerate(stats_range, start=PLAYER_DATA_START_ROW):
        if player[0] != " ":
            for col_index, col in enumerate(team_stat_columns):
                stats_collection[col].append((player[col_index], player[1]))
        else:
            if team_abbr not in empty_rows:
                empty_rows[team_abbr] = []
            empty_rows[team_abbr].append(row_index)
        print(f"Scraped {team_abbr} team sheet.")


# calculates percentiles (weighted by minutes played) for each stat category
def calculate_weighted_percentiles(stats_collection):
    print("Calculating percentiles.")
    percentiles_dict = {}

    for col, stats in stats_collection.items():
        values = np.array([stat[0] for stat in stats], dtype=float)
        mp = np.array([stat[1] for stat in stats], dtype=float)

        sorted_indices = np.argsort(values)
        sorted_values = values[sorted_indices]
        sorted_mp = mp[sorted_indices]

        cumulative_mp = np.cumsum(sorted_mp)
        total_mp = cumulative_mp[-1]

        percentiles = []
        for i in range(len(sorted_values)):
            percentile = 100 * (cumulative_mp[i] - sorted_mp[i] / 2) / total_mp
            percentiles.append(percentile)

        original_indices = np.argsort(sorted_indices)
        percentiles = np.array(percentiles)[original_indices]

        percentiles_dict[col] = percentiles.tolist()

    print("Percentiles calculated.")
    return percentiles_dict


# convert percentiles into a green --> yellow --> red gradient color scale
def percentile_to_color(percentile):
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "", ["#ff5b38", "#fbf841", "#27a62b"]
    )
    norm = mcolors.Normalize(vmin=0, vmax=100)
    percentile = max(0, min(percentile, 100))
    return mcolors.to_hex(cmap(norm(percentile)))


def reset_background_color(team_sheet, rows_to_clear):
    batch_color_removals = []
    for row in rows_to_clear:
        for col in range(PLAYER_STATS_START_COLUMN_NUM, PLAYER_STATS_END_COLUMN_NUM):
            batch_color_removals.append(
                {
                    "range": rowcol_to_a1(row, col),
                    "format": {
                        "backgroundColor": None,
                    },
                }
            )
    if batch_color_removals:
        team_sheet.batch_format(batch_color_removals)


def apply_percentile_colors(percentiles_dict):
    team_sheet = sheet.worksheet(team_abbr)
    print(team_abbr)
    master_player_ids = master_sheet.col_values(MASTER_ID_COLUMN_NUM)
    team_colorings = []
    master_colorings = []
    for row_index, player_id in enumerate(
        team_sheet.col_values(PLAYER_STATS_END_COLUMN_NUM)[
            PLAYER_DATA_START_ROW - 1 : PLAYER_DATA_END_ROW
        ],
        start=PLAYER_DATA_START_ROW,
    ):
        if (
            team_abbr in empty_rows
            and row_index not in empty_rows[team_abbr]
            or team_abbr not in empty_rows
        ):
            for master_row_index, id in enumerate(
                master_player_ids, start=MASTER_DATA_START_ROW
            ):
                if id == player_id:
                    master_row = master_row_index
                    break

            for col_index, col in enumerate(team_stat_columns):
                column_percentiles = percentiles_dict[col]

                percentile = column_percentiles[0]
                percentiles_dict[col].pop(0)
                if col in REVERSED_TEAM_STATS_COLUMNS:
                    percentile = 100 - percentile

                color = percentile_to_color(percentile)
                rgb = mcolors.to_rgb(color)

                team_colorings.append(
                    {
                        "range": f"{col}{row_index}",
                        "format": {
                            "backgroundColor": {
                                "red": rgb[0],
                                "green": rgb[1],
                                "blue": rgb[2],
                            }
                        },
                    }
                )

                master_colorings.append(
                    {
                        "range": f"{rowcol_to_a1(master_row + MASTER_DATA_START_ROW, PLAYER_STATS_START_COLUMN_NUM + 1)}",
                        "format": {
                            "backgroundColor": {
                                "red": rgb[0],
                                "green": rgb[1],
                                "blue": rgb[2],
                            }
                        },
                    }
                )

    team_sheet.batch_format(team_colorings)
    master_sheet.batch_format(master_colorings)
    print(f"{team_abbr} color coding applied.")


# shortened_team_sheets = team_sheets[team_sheets.index("UTA") :]

for team_abbr in team_sheets:
    update_team_sheet(team_abbr)
    time.sleep(30)
    print("Sleeping for 30 seconds.")

if removed_players:
    update_removed_players(removed_players)

for team_abbr in team_sheets:
    scrape_team_sheets(team_abbr)

percentiles_dict = calculate_weighted_percentiles(stats_collection)

for team_abbr in team_sheets:
    apply_percentile_colors(percentiles_dict)
    time.sleep(15)
