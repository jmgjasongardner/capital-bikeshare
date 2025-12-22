library(dplyr)
library(arrow)
library(ggmap)
library(ggplot2)
library(gganimate)
o <- read_parquet('data/201911.parquet')
n <- read_parquet('data/202411.parquet')
s <- read_parquet('data/bike_stations.parquet') %>% group_by(start_station_id) %>% top_n(1) %>% ungroup() %>% select(station_id = start_station_id,
                                                                                                                     lat = start_lat,
                                                                                                                     lng = start_lng)
o <- o %>%
  arrange(`Start date`, `End date`) %>%
  group_by(`Bike number`) %>%
  mutate(
    MonthBikeRideNum = row_number(),
    StartSameAsPriorDrop =
      as.numeric(`Start station number` == lag(`End station number`))
  ) %>%
  left_join(s, by = c('Start station number' = 'station_id'), suffix = c('', '_start')) %>%
  left_join(s, by = c('End station number' = 'station_id'), suffix = c('', '_end'))
onebike <- o %>% filter(`Bike number` == '78524')




df <- o %>% arrange(MonthBikeRideNum)

ggplot(df) +
  geom_segment(
    aes(x = lng, y = lat, xend = lng_end, yend = lat_end,
        color = MonthBikeRideNum),
    arrow = arrow(length = unit(0.15, "cm")),
    size = 1
  ) +
  scale_color_viridis_c(option = "magma") +
  coord_equal() +
  labs(
    title = "Bike Ride Paths",
    x = "Longitude", y = "Latitude"
  ) +
  theme_minimal()







df <- onebike %>% arrange(MonthBikeRideNum)

# Bounding box for the map
lat_range <- range(c(df$lat, df$lat_end), na.rm = TRUE)
lng_range <- range(c(df$lng, df$lng_end), na.rm = TRUE)

bike_map1 <- get_stadiamap(
  bbox = c(
    left   = lng_range[1],
    bottom = lat_range[1],
    right  = lng_range[2],
    top    = lat_range[2]
  ),
  zoom = 13,
  maptype = "stamen_terrain"
)

ggmap(bike_map1) +
  geom_segment(
    data = onebike,
    aes(x = lng, y = lat, xend = lng_end, yend = lat_end,
        color = MonthBikeRideNum),
    linewidth = 0.7,
    arrow = arrow(length = unit(0.15, "cm"))
  ) +
  scale_color_viridis_c(option = "plasma") +
  labs(
    title = "Bike Movement Path",
    subtitle = "Each segment is a ride; colors show order",
    x = "", y = ""
  ) +
  theme_minimal()
  




df <- onebike %>% arrange(MonthBikeRideNum)

p <- ggplot(df) +
  geom_segment(
    aes(x = lng, y = lat, xend = lng_end, yend = lat_end,
        frame = MonthBikeRideNum),
    arrow = arrow(length = unit(0.2, "cm")),
    color = "steelblue", linewidth = 1
  ) +
  labs(title = "Bike Movement Animation", x = "", y = "")

animate(p, nframes = nrow(df), fps = 2, width = 800, height = 600)
