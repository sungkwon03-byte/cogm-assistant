# E2E PASS 리포트 (스냅샷: 2025-10-09 09:52:52Z)

| File | Rows | Cols | year(min–max) | Head(12) |
|---|---:|---:|:---:|---|
| statcast_features_player_year.csv | 102825 | 43 | 2015–2015 | year, mlbam, player_name, Pitches, PA, Swings, Whiffs, Z_Pitches, O_Pitches, Z_Swings, O_Swings, Z_Whiffs |
| statcast_pitch_mix_detailed.csv | 57491 | 20 | 1817–2025 | year, role, mlbam, player_name, pitch_type, pitches, usage_rate, zone_rate, whiff_rate, z_whiff_rate, o_whiff_rate, chase_rate |
| statcast_pitch_mix_detailed_plus_bat.csv | 34307 | 33 | 2015–2025 | role, year, mlbam, pitch_type, segment, vhb, pitches, Z_Pitches, O_Pitches, Z_Swings, O_Swings, Z_Whiffs |
| count_tendencies_bat.csv | 2595 | 19 | 1801–2990 | year, mlbam, vhb, pitches, zone_rate, z_swing_rate, o_swing_rate, z_contact_rate, o_contact_rate, z_csw_rate, chase_rate, edge_rate |
| bat_stability.csv | 1437 | 3 | – | player_id, metric, rolling_var |
| weakness_map_player_year.csv | 965 | 15 | 1801–2990 | year, player_mlbam, pitches, zc, zcsw, chase, edge, heart, zone, weak_zone_edge, heart_chase_idx, player_id |
| trend_3yr.csv | 115450 | 15 | 1871–2024 | year, playerID, BABIP, BBK, OPS_plus_approx, BABIP_3yr, BBK_3yr, OPSp_3yr, avg_ev, whiff_rate, player_id, season |
| trade_value.csv | 163093 | 12 | 1876–2024 | year, teamID, playerID, player_name, WAR, salary, salaryMM, WAR_per_$MM, player_id, name, value, surplus |
| mock_trades_mvp.csv | 200 | 5 | – | trade_id, team_from, team_to, players_out, players_in |
| fa_market_mvp.csv | 400 | 15 | 2015–2015 | year, role, mlbam, player_name, perf, pct, war_est, AAV_mid, AAV_low, AAV_high, years_guess, player_id |
| advanced_metrics.csv | 154825 | 16 | 1871–2024 | year, playerID, role, PA, AVG, SLG, ISO, BABIP, BB_pct, K_pct, wOBA, IP |
| league_runenv.csv | 287 | 13 | 1821–2982 | year, lgID, G, R, HR, BB, SO, R_per_G, HR_per_G, BB_per_G, SO_per_G, season |
| ump_euz_indices.csv | 11 | 6 | 2015–2025 | year, csr_edge, csr_heart, euz_index, season, umpire_id |
| mart_star_idfix.csv | 20181 | 4 | – | player_id, mlb_id, bbref_id, fg_id |