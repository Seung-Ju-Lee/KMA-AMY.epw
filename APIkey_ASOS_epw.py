#%%
# #? ASOS 데이터 호출
import requests
import pandas as pd
from io import StringIO
import sys
import calendar # 월별 마지막 날짜 계산을 위해 추가

# EPW 생성용 모듈 호출
from SolarGeometry import watanabe
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


#%% #! 01. 기본 설정

APIKEY         = 'CwpUCpOrQ3eKVAqTqyN3pw' # 승주key #?API KEY 발급 후 입력
targetyear     = '2024'                # 추출하고자 하는 연도 입력
# option         = '' #"station_num", "all_station", "address"

# station_info   = pd.read_csv('META_관측지점정보_종료지점제외(20251222기준).csv', delimiter=',', encoding='euc-kr').loc[6]
station_num    = '108'
station        = station_num                   # 지역번호 입력 (기상청코드)
save_file_path = f'./예시/ASOS/{station}_{targetyear}.csv' # 저장 파일명 변경
save_file_path = f'C:/Users/seungju/Desktop/causal calibration/260107 IAQbaseline test/{station}_{targetyear}.csv' # 저장 파일명 변경


#%% #! API URLs
#? Surface observed data (temperature, etc)
BASE_URL = 'https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php?tm1={starttime}&tm2={endtime}&stn={station}&help=1&authKey={APIKEY}'

#? Station data
STATION_URL = 'https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?inf=SFC&stn=&tm={targetyear}12310900&help=1&authKey={APIKEY}'


#%% #! Parsing header data (URL -> data)
#? Surface observed data header name
surface_data_headername = """
TM STN WD WS GST_WD GST_WS GST_TM PA PS PT PR TA TD HM PV RN
RN_DAY RN_JUN RN_INT SD_HR3 SD_DAY SD_TOT WC WP WW CA_TOT CA_MID
CH_MIN CT CT_TOP CT_MID CT_LOW VS SS SI ST_GD TS TE_005 TE_01
TE_02 TE_03 ST_SEA WH BF IR IX
""".split()




#%% #! Fetching functions
#? Station data fetching
def get_station_info(targetyear, api_key, station_url, save_path=None):
    print(f"{targetyear}년 기준 지점정보 수집 진행중")

    #? Station data header name (URL -> data)
    station_data_headername = """
    STN_ID LON LAT STN_SP HT HT_PA HT_TA HT_WD HT_RN STN_KO STN_EN
    STN_AD FCT_ID LAW_ID BASIN LAW_ADDR
    """.split()
    
    #? valid url
    url = station_url.format(
        targetyear=targetyear,
        APIKEY=api_key
    )

    #? fetch url
    res = requests.get(url, timeout=30)
    res.raise_for_status()

    try:
        text = res.content.decode("euc-kr") # Korean decoded using "euc-kr"
    except UnicodeDecodeError:
        text = res.text

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    data_lines = [line for line in lines if not line.startswith("#") and "END" not in line.upper()]

    rows = [line.split(maxsplit=len(station_data_headername)-1) for line in data_lines]
    df = pd.DataFrame(rows, columns=station_data_headername)

    if save_path is not None:
        df.to_csv(save_path, index=False, encoding="utf-8-sig")
        print(f"[저장완료] '{save_path}'")

    print("[호출완료] station info")
    

    return df

station_df = get_station_info(targetyear, APIKEY, STATION_URL)


#%% #! 02. 월간 데이터 호출 (00:00 ~23:00)
def fetch_kma_data_month(file_url, column_names):
  try:
    response = requests.get(file_url)
    response.raise_for_status()        # HTTP 4xx/5xx 에러 체크

    # 인코딩 처리
    try:
      decoded_content = response.content.decode('euc-kr')
    except UnicodeDecodeError:
      decoded_content = response.text 
      
    csv_data = StringIO(decoded_content)
    
    df = pd.read_csv(
      csv_data, 
      sep='\s+',      
      comment='#',     
      header=None,     
      names=column_names, 
      encoding='utf-8',
      skipinitialspace=True
    )
    
    return df

  except Exception as e:
    print(f"데이터 호출/파싱 중 오류 발생: {e}", file=sys.stderr)
    return pd.DataFrame()

#%% #! 30. 1년치 데이터 호출 및 합치기 (1월 1일 00:00 - 12월 31일 23:00)
def get_full_year_data(year, station, api_key, base_url, column_names, save_path):
  all_data_frames = []
  
  print(f"{station}_{year} 데이터 수집 진행중")
  
  for month in range(1, 13):
    month_str = str(month).zfill(2)
    
    #? 월별 마지막 날짜 계산
    _, last_day = calendar.monthrange(int(year), month)
    if month == 2: # 윤년 제외 (epw 오류 방지)
        last_day = 28
    
    day_str = str(last_day).zfill(2)
    
    # 시작 및 종료 시간 문자열 생성 (0000시부터 2300시까지)
    starttime = f'{year}{month_str}010000'       # 매월 1일 00시 00분
    endtime  = f'{year}{month_str}{day_str}2300' # 해당 월 마지막 날 23시 00분
    
    # API url 생성
    url = base_url.format(
      starttime=starttime,
      endtime=endtime,
      station=station,
      APIKEY=api_key
    )
    
    # 월별 데이터 호출
    monthly_df = fetch_kma_data_month(url, column_names)
    
    if not monthly_df.empty:
      all_data_frames.append(monthly_df)
      print(f"[호출완료] {year}-{month_str}")
    else:
      print(f"[호출실패] {year}-{month_str}")

  # 12개월 데이터를 하나의 DataFrame으로 통합
  if all_data_frames:
    print("\n=================================================")
    print("        데이터 통합 및 저장        ")
    print("=================================================")
    final_df = pd.concat(all_data_frames, ignore_index=True)
    
    # 최종 DataFrame을 CSV로 저장 (헤더 포함)
    final_df.to_csv(save_path, index=False, encoding='utf-8')
    
    print(f"[저장완료] '{save_path}'")
    print("\n--- 최종 DataFrame 처음 5줄 (헤더 포함) ---")
    print(final_df.head())
    return final_df
  else:
    print("[실패] 모든 월의 데이터 수집에 실패하여 저장할 데이터가 없습니다.")
    return pd.DataFrame()

#%% #! 04. 함수 실행 (파일명:'지점번호_연도.csv')
full_data_frame = get_full_year_data(
  targetyear, 
  station, 
  APIKEY, 
  BASE_URL, 
  surface_data_headername, 
  save_file_path
)

#%%
# station_info = pd.read_csv(r"B:\공유 드라이브\05 Archive\41 코드\기상데이터 기반 EPW 파일 생성\META_관측지점정보_종료지점제외(20251023기준).csv", delimiter=',', encoding='euc-kr')



#%%

from SolarGeometry import watanabe
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


# EPW 컬럼 매핑 (ASOS 원본 컬럼명: EPW 내부 사용 컬럼명)
ASOS_COLUMN_MAP = {
    'TM': '일시',            # Date/Time (index)
    'TA': '기온(°C)',        # Dry Bulb Temperature
    'HM': '습도(%)',         # Relative Humidity
    'PA': '현지기압(hPa)',    # Atmospheric Station Pressure
    'SI': '일사(MJ/m2)',      # Global Horizontal Radiation
    'WD': '풍향(16방위)',     # Wind Direction
    'WS': '풍속(m/s)',       # Wind Speed
}


def ep_datetime_to_dt(year, month, day, hour, minute):
    """
    EPW 파일 데이터 라인에서 시간을 분해하여 datetime 객체로 변환합니다.
    """
    dt_list = [int(year), int(month), int(day), int(hour), int(minute)]
    if dt_list[3] < 24:
        return datetime(*dt_list[:5])
    else:
        # 시간 24시는 다음 날 0시로 처리
        dt_list[3] = 0
        return datetime(*dt_list[:5]) + timedelta(days=1)


def generate_epw(asos_file, base_epw, output_epw):
    """ASOS 데이터와 정보를 사용하여 EPW 파일을 생성합니다."""
    
    station_num = station_info['지점']
    wmo = int('47' + f'{station_num:03d}' + '0') 
    
    params = {
        'city': station_info['지점명'],
        'region': station_info['지점명'], # 지역 코드. 필요시 수정
        'wmo': wmo,
        'latitude': station_info['위도'],
        'longitude': station_info['경도'],
        'timezone': 9.0, # 한국 표준시
        'elevation': station_info['노장해발고도(m)']
    }
    print(f"--- 1. EPW 헤더 정보 ---")
    print(f"위치: {params['city']} ({params['region']}), 위도: {params['latitude']:.2f}, 고도: {params['elevation']:.1f}m")

    # 2. ASOS 데이터 로드 및 전처리
    asos_path = Path(asos_file).resolve()
    
    # TM 컬럼을 문자열로 로드하여 지수 표기법 문제 해결
    asos = pd.read_csv(str(asos_path), encoding='euc_kr', header=0, dtype={'TM': str})
    print("\n--- [데이터 체크] 일사량(SI) 컬럼의 통계 ---")
    print(asos['SI'].describe()) 
    print("일사량이 0보다 큰 데이터 개수:", (asos['SI'] > 0).sum())
    asos = asos.rename(columns=ASOS_COLUMN_MAP)
    
    # 12자리 문자열 시간을 DatetimeIndex로 변환
    asos['일시'] = pd.to_datetime(asos['일시'], format='%Y%m%d%H%M')
    asos = asos.set_index('일시')
    
    # -9를 0으로, NaN을 0으로 처리 후 시간 보간
    asos['일사(MJ/m2)'] = asos['일사(MJ/m2)'].replace(-9, 0).fillna(0)
    asos.interpolate(method='time', inplace=True) 

    asos_year = asos.index[0].year
    dt_range = (asos.index[0], asos.index[-1])
    print(f"--- 2. ASOS 데이터 로드 ---")
    print(f"시작 시간: {dt_range[0]}, 종료 시간: {dt_range[1]}")
    print(f"첫 5개 ASOS 데이터:\n{asos[['기온(°C)', '습도(%)', '일사(MJ/m2)']].head()}")

    # 3. Base EPW 로드 및 인덱스 정렬
    base_epw_path = Path(base_epw).resolve()
    with open(str(base_epw_path), 'r', encoding='utf-8') as file:
        epw_lines = file.readlines()

    epw_dts = [
        ep_datetime_to_dt(asos_year, *line.split(',')[1:5])
        for line in epw_lines if line.startswith('20')
    ]
    interpolated_asos = asos.reindex(asos.index.union(epw_dts)).interpolate(method='time')

    # 4. EPW 내용 업데이트 (헤더 및 데이터)
    lsm = params['timezone'] * 15 # Local Standard Meridian
    
    # 디버깅을 위한 카운터 설정 (최대 10개 데이터 라인만 자세히 출력)
    DEBUG_LIMIT = 10
    debug_counter = 0
    print(f"\n--- 3. EPW 데이터 라인 업데이트 시작 (처음 {DEBUG_LIMIT}개 라인 디버그) ---")
    
    for l_idx, line in enumerate(epw_lines):
        cols = line.split(',')
        
        # LOCATION 및 COMMENTS 업데이트 (생략)
        if line.lower().startswith('location'):
            cols[1], cols[2], cols[5] = params['city'], params['region'], str(params['wmo'])
            cols[6], cols[7], cols[8], cols[9] = str(params['latitude']), str(params['longitude']), str(params['timezone']), str(params['elevation']).strip()
            epw_lines[l_idx] = ','.join(cols) + '\n'
        elif line.lower().startswith('comments 1'):
            epw_lines[l_idx] = f'COMMENTS 1, "Based on {base_epw_path.name}"\n'
        elif line.lower().startswith('comments 2'):
            epw_lines[l_idx] = f'COMMENTS 2, "Using data {asos_path.name} from {dt_range[0].strftime("%Y-%m-%d")} to {dt_range[1].strftime("%Y-%m-%d")}"\n'

        # WEATHER DATA 업데이트
        elif line.startswith('20'):
            is_debugging = debug_counter < DEBUG_LIMIT
            
            # 마지막 필드의 개행 문자 제거: cols[33] 이후에 다른 데이터가 올 수 있으므로 strip 처리
            cols[-1] = cols[-1].strip()

            dt = ep_datetime_to_dt(asos_year, *cols[1:5])
            if dt < dt_range[0] or dt_range[1] < dt: 
                epw_lines[l_idx] = ','.join(cols) + '\n' # 원본 데이터 유지
                continue

            values = interpolated_asos.loc[dt, :]
            
            if values.empty:
                if is_debugging:
                    print(f"[ERROR] {dt} 시점에 매칭되는 ASOS 데이터가 없습니다. (Raw Index Error)")
                continue

            if is_debugging:
                print(f"\n--- [DEBUG] Line {l_idx + 1} ({dt.strftime('%Y-%m-%d %H:%M')}) ---")
                
            # 0. Year 업데이트
            cols[0] = str(asos_year)
            
            # 1. 기온, 습도, 기압 업데이트 (필드 6, 8, 9)
            temp_c = values['기온(°C)']
            rh_pct = values['습도(%)']
            pres_hpa = values['현지기압(hPa)']
            
            cols[6] = str(temp_c)
            cols[8] = str(rh_pct)
            cols[9] = str(pres_hpa * 100) # [Pa] 변환
            
            # 2. 일사량 계산 및 업데이트 (필드 13, 14, 15)
            ghi_mj = values['일사(MJ/m2)']
            ghi = ghi_mj * (1000000 / 3600) # MJ/hr -> Wh/m2
            Ibh, Idh, In = watanabe(params['latitude'], params['longitude'], lsm, dt, ghi)
            
            cols[13] = str(ghi) 
            cols[14] = str(In)  
            cols[15] = str(Idh) 
            
            # 3. 풍향, 풍속 업데이트 (필드 20, 21)
            cols[20] = str(values['풍향(16방위)'])
            cols[21] = str(values['풍속(m/s)'])
            if ghi > 0:
                print(f"[{dt}] 입력GHI: {ghi:.1f} | 계산DNI(직달): {In:.1f} | 계산DHI(확산): {Idh:.1f}")
                
                # 만약 GHI는 높은데 직달일사(DNI)가 0이라면 계산 로직 오류입니다.
                if ghi > 200 and In == 0:
                    print(f"   ⚠️ 경고: {dt.hour}시에 일사량은 높은데 직달성분이 0입니다! 고도각 계산을 확인해야 합니다.")
            # EPW 라인 완성
            epw_lines[l_idx] = ','.join(cols) + '\n'

            if is_debugging:
                print(f"ASOS -> Temp: {temp_c:.2f}C, RH: {rh_pct:.1f}%, Pres: {pres_hpa:.1f}hPa")
                print(f"SOLAR -> GHI: {ghi_mj:.2f}MJ/m2 -> {ghi:.2f}Wh/m2 (DNI: {In:.2f}, DHI: {Idh:.2f})")
                print(f"EPW OUTPUT START: {epw_lines[l_idx].strip()[:100]}...")
                debug_counter += 1

    # 5. EPW 파일 저장
    output_path = Path(output_epw).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True) 
    with open(str(output_path), 'w', encoding='utf-8') as file:
        file.write(''.join(epw_lines))
        
    print(f"\n [EPW 생성 완료]:{output_path.name}")


#? 실행 부분
# if __name__ == '__main__':
    
#     # ----------------------------------------------------
#     # 👇 파일 경로를 여기에 직접 입력하세요.
#     # ----------------------------------------------------
#     ASOS_FILE = save_file_path
#     BASE_EPW_FILE = './예시/KOR_SO_Seoul.WS.471080_TMYx.epw' 
#     OUTPUT_EPW_FILE = f'C:/Users/seungju/Desktop/causal calibration/260107 IAQbaseline test/{station}_{targetyear}.epw'

#     generate_epw(
#         asos_file=ASOS_FILE,
#         base_epw=BASE_EPW_FILE,
#         output_epw=OUTPUT_EPW_FILE
#     )
