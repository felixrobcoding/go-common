package utiltools

import (
	"strconv"
	"time"
)

const (
	YEAR_TIME_FORMAT     = "2006"
	MONTH_TIME_FORMAT    = "2006-01"
	DAY_TIME_FORMAT      = "2006-01-02"
	TIME_STANDARD_LAYOUT = "2006-01-02 15:04:05"
)

var (
	TimeZone, _     = time.LoadLocation("Asia/Shanghai")
	WeekDayMap      = map[string]int{"Monday": 1, "Tuesday": 2, "Wednesday": 3, "Thursday": 4, "Friday": 5, "Saturday": 6, "Sunday": 7}
	MonthQuarterMap = map[string]string{"01": "01", "02": "01", "03": "01", "04": "04", "05": "04", "06": "04",
		"07": "07", "08": "07", "09": "07", "10": "10", "11": "10", "12": "10"}
)

// TimeFormat 时间转字符串
func TimeFormat(timeObj time.Time) string {
	return timeObj.Format(TIME_STANDARD_LAYOUT)
}

// TimeParse 字符串转时间
func TimeParse(timeStr string) (time.Time, error) {

	result, err := time.Parse(TIME_STANDARD_LAYOUT, timeStr)
	if err != nil {
		return time.Now(), err
	}
	return result, nil
}

// UnixToFormat 时间戳格式转日期
func UnixToFormat(unix int64) string {
	return time.Unix(unix, 0).Format(TIME_STANDARD_LAYOUT)
}

// YearUnixToFormat 年时间戳格式转年格式
func YearUnixToFormat(unix int64) string {
	return time.Unix(unix, 0).Format(YEAR_TIME_FORMAT)
}

// MonthUnixToFormat 月时间戳格式转月格式
func MonthUnixToFormat(unix int64) string {
	return time.Unix(unix, 0).Format(MONTH_TIME_FORMAT)
}

// DayUnixToFormat 日时间戳格式转日格式
func DayUnixToFormat(unix int64) string {
	return time.Unix(unix, 0).Format(DAY_TIME_FORMAT)
}

// DayFormatToUnix 日格式转时间戳
func DayFormatToUnix(format string) int64 {
	dayTime, _ := time.ParseInLocation(DAY_TIME_FORMAT, format, TimeZone)
	return dayTime.Unix()
}

// NowWeekUnix 获取入参时间的周时间戳
func NowWeekUnix(unix int64) int64 {
	unixTime := time.Unix(unix, 0)

	intervalWeek := WeekDayMap[unixTime.Weekday().String()] - 1
	intervalHour, _ := time.ParseDuration("-" + strconv.Itoa(24*intervalWeek) + "h")

	weekTime, _ := time.ParseInLocation(DAY_TIME_FORMAT, unixTime.Add(intervalHour).Format(DAY_TIME_FORMAT), TimeZone)
	return weekTime.Unix()
}

// NowYearUnix 获取入参时间的年度时间戳
func NowYearUnix(unix int64) int64 {
	yearTime, _ := time.ParseInLocation(YEAR_TIME_FORMAT, time.Unix(unix, 0).Format(YEAR_TIME_FORMAT), TimeZone)
	return yearTime.Unix()
}

// NowMonthUnix 获取入参时间的月度时间戳
func NowMonthUnix(unix int64) int64 {
	monthTime, _ := time.ParseInLocation(MONTH_TIME_FORMAT, time.Unix(unix, 0).Format(MONTH_TIME_FORMAT), TimeZone)
	return monthTime.Unix()
}

// NowDayUnix 获取入参时间的日度时间戳
func NowDayUnix(unix int64) int64 {
	dayTime, _ := time.ParseInLocation(DAY_TIME_FORMAT, time.Unix(unix, 0).Format(DAY_TIME_FORMAT), TimeZone)
	return dayTime.Unix()
}

// NowQuarterUnix 获取入参时间的季度时间戳
func NowQuarterUnix(unix int64) int64 {
	month := time.Unix(unix, 0).Format("01")
	monthTime, _ := time.ParseInLocation(MONTH_TIME_FORMAT, time.Unix(unix, 0).Format(YEAR_TIME_FORMAT)+"-"+MonthQuarterMap[month], TimeZone)
	return monthTime.Unix()
}

// ComputeDayTimeUnix 计算天的时间戳
func ComputeDayTimeUnix(day int) int64 {
	return int64(60 * 60 * 24 * day)
}

// TimePoint 获取指定时间到今天凌晨之间的时间点
func TimePoint(unix, endUnix, interval int64) []string {
	var results []string

	for {
		if (endUnix - interval) > unix {
			results = append(results, strconv.FormatInt(endUnix, 10)+"-"+strconv.FormatInt(endUnix-interval, 10))
			endUnix = endUnix - interval
		} else {
			results = append(results, strconv.FormatInt(endUnix, 10)+"-"+strconv.FormatInt(unix, 10))
			break
		}
	}

	return results
}

// SpanYear 获取两时间之间是否跨年,以及跨越多少年
func SpanYear(unix, endUnix int64) []string {
	var results []string

	for {
		unixFormat := time.Unix(unix, 0).Format(YEAR_TIME_FORMAT)
		endUnixFormat := time.Unix(endUnix, 0).Format(YEAR_TIME_FORMAT)
		if (endUnix - unix) >= 31622400 {
			results = append(results, time.Unix(unix, 0).Format(YEAR_TIME_FORMAT))
			unix = unix + 31622400
		} else if unixFormat != endUnixFormat {
			results = append(results, time.Unix(unix, 0).Format(YEAR_TIME_FORMAT))
			results = append(results, time.Unix(endUnix, 0).Format(YEAR_TIME_FORMAT))
			break
		} else {
			results = append(results, time.Unix(endUnix, 0).Format(YEAR_TIME_FORMAT))
			break
		}
	}

	return results
}

// SpanMonth 获取两时间之间是否跨月,以及跨越多少月
func SpanMonth(unix, endUnix int64) []string {
	var results []string
	var monthFormat = "200601"

	for {
		// 开始时间和结束时间
		unixFormat := time.Unix(unix, 0).Format(monthFormat)
		endUnixFormat := time.Unix(endUnix, 0).Format(monthFormat)
		if (endUnix - unix) >= 2678400 {
			results = append(results, time.Unix(unix, 0).Format(monthFormat))
			unix = unix + 2678400
		} else if unixFormat != endUnixFormat {
			results = append(results, time.Unix(unix, 0).Format(monthFormat))
			results = append(results, time.Unix(endUnix, 0).Format(monthFormat))
			break
		} else {
			results = append(results, time.Unix(endUnix, 0).Format(monthFormat))
			break
		}
	}

	return results
}
