def get_weather_icon(temp):
    temp = float(temp)
    if temp < 25:
        return "🧊"
    elif temp < 30:
        return "🌤️"
    else:
        return "🔥"
