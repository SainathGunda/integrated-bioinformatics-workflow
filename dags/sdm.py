import numpy as np # Imports numpy for numerical operations
import pandas as pd # Imports pandas for working with tabular data like CSV files
import matplotlib.pyplot as plt # Imports matplotlib for creating plots and figures
import geopandas as gpd # Imports geopandas for working with spatial/geographic data
from datetime import datetime # Imports datetime to define Airflow DAG start dates
from pygbif import occurrences as occ # Imports GBIF occurrence tools to download species occurrence records
import rioxarray as rxr # Imports rioxarray to open and process raster climate files
from pathlib import Path # Imports Path to handle file and folder paths safely
import seaborn as sns 
from shapely.geometry import Point # Imports Point to create geometry points from longitude and latitude

# Imports DAG class to create an Airflow workflow
from airflow import DAG

# Imports EmailOperator to send email notifications from Airflow
from airflow.providers.smtp.operators.smtp import EmailOperator

# Imports PythonOperator to run Python functions as Airflow tasks
from airflow.providers.standard.operators.python import PythonOperator
import yaml # Imports yaml to read configuration values from a YAML file
import elapid as ela # Imports elapid for MaxEnt species distribution modeling
# Imports train_test_split to split data into training and testing sets
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve, auc # Imports roc_curve and auc to evaluate model performance

# Path to the YAML configuration file
CONFIG_FILE = Path("/app/config/sdm_config.yaml")
with open(CONFIG_FILE, "r") as f: # Opens and reads the YAML configuration file
    config = yaml.safe_load(f) # Converts the YAML file content into a Python dictionary

species_name=config['species_name']
output_dir=config['output_dir']
num_of_bg_points=config['num_of_background_points']
iucn_input=config['iucn_input'] # Reads the IUCN input folder path from the config file
bio_vars=config['bio_variables'] # Reads the selected bioclimatic variables from the config file
# Creates a species-specific output directory path
# Example: "Semnopithecus entellus" becomes "Semnopithecus_entellus"
species_output_dir = Path(output_dir) / Path(species_name.replace(" ", "_"))
# Creates the species output folder if it does not already exist
# parents=True creates missing parent folders also
# exist_ok=True avoids an error if the folder already exists
species_output_dir.mkdir(parents=True, exist_ok=True)
# URL for the Natural Earth countries shapefile
# This file contains country boundary data used for map backgrounds
url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip"
# Reads the country boundary shapefile directly from the URL
# The result is stored as a GeoDataFrame
world = gpd.read_file(url)

# Function to download species occurrence records from GBIF
def gbif_occurrences():
    all_results = []
    offset = 0
    target = config['num_of_records_to_pull_from_gbif'] # Number of records we want to collect from GBIF

    while len(all_results) <= target:
        # Searches GBIF for occurrence records of the selected species
        batch = occ.search(
        scientificName=species_name,
        hasCoordinate=True,
        offset=offset)

        results = batch["results"]
        if not results:    # stops if no more records
            break
        all_results.extend(results)
        offset += 300
    # Converts all downloaded GBIF records into a DataFrame
    occ_data = pd.DataFrame(all_results)
    # Saves the full GBIF occurrence data to a CSV file
    occ_data.to_csv(f"{species_output_dir}/gbif_occurrences.csv", index=False)

    coords = (occ_data[["decimalLongitude", "decimalLatitude"]]
        .rename(columns={"decimalLongitude": "lon", "decimalLatitude": "lat"}).copy())
    # Removes rows with missing coordinates and duplicate coordinate pairs
    coords_cleaned=coords.dropna().drop_duplicates()
    # Save the cleaned coordinates to a CSV file
    coords_cleaned.to_csv(f"{species_output_dir}/gbif_coordinates_cleaned.csv", index=False)
    
# Function to load and filter IUCN range data for the selected species
def load_iucn():
    # Reads the IUCN mammal shapefiles
    iucn1 = gpd.read_file(f"{iucn_input}/MAMMALS_PART1.shp")
    iucn2 = gpd.read_file(f"{iucn_input}/MAMMALS_PART2.shp")
    # Filters to keep only rows matching the selected species name
    iucn_filtered_1 = iucn1[iucn1["sci_name"].str.lower() == species_name.lower()]
    iucn_filtered_2 = iucn2[iucn2["sci_name"].str.lower() == species_name.lower()]
    # Combines the filtered rows from both IUCN files into one GeoDataFrame
    iucn = pd.concat([iucn_filtered_1, iucn_filtered_2], ignore_index=True)
    # Saves the filtered IUCN range as a GeoJSON file
    iucn.to_file(f"{species_output_dir}/filtered_iucn.geojson", driver="GeoJSON")
    
# Function to plot GBIF points, IUCN range, and validate points inside the species range
def plot_iucn_and_gbif():
    # Loads the filtered IUCN range polygon file
    iucn = gpd.read_file(f"{species_output_dir}/filtered_iucn.geojson")
    # Loads the cleaned GBIF coordinate CSV file
    coords_cleaned=pd.read_csv(f"{species_output_dir}/gbif_coordinates_cleaned.csv")
    # Converts the GBIF coordinates into a GeoDataFrame with point geometry
    coords_cleaned=gpd.GeoDataFrame(coords_cleaned,
        geometry=gpd.points_from_xy(coords_cleaned["lon"], coords_cleaned["lat"]), crs="EPSG:4326")
    
    # Gets map bounds from the GBIF points using the 2nd and 98th percentiles
    # This helps avoid extreme outlier points affecting the map view too much
    xmin = coords_cleaned["lon"].quantile(0.02)
    xmax = coords_cleaned["lon"].quantile(0.98)
    ymin = coords_cleaned["lat"].quantile(0.02)
    ymax = coords_cleaned["lat"].quantile(0.98)
    # Adds padding around the map so points are not too close to the edges
    xpad = max((xmax - xmin) * 0.5, 1)
    ypad = max((ymax - ymin) * 0.5, 1)
    # Final x-axis and y-axis limits for all maps
    xlim = (xmin - xpad, xmax + xpad)
    ylim = (ymin - ypad, ymax + ypad)

    # ---- GBIF occurrence points map ----
    fig, ax = plt.subplots(figsize=(10, 8))
    world.plot(ax=ax, color="grey", linewidth=1, edgecolor="black") # Plots the world map as background
    coords_cleaned.plot(ax=ax, color="red", markersize=20) # Plots GBIF points in red
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_title(f"{species_name} Occurrence Points From GBIF")
    plt.savefig(f"{species_output_dir}/Occurrence Points From GBIF.png", dpi=300, bbox_inches="tight") # Saves the figure
    plt.close()

    # ---- IUCN polygon map ----
    fig, ax = plt.subplots(figsize=(10, 8))
    world.plot(ax=ax, color="grey", linewidth=1, edgecolor="black") # Plots the world map as background
    iucn.plot(ax=ax, color="red", alpha=0.5, edgecolor="darkred") # Plots the IUCN range polygon in red
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_title(f"{species_name} IUCN Polygon")
    plt.savefig(f"{species_output_dir}/IUCN Polygon.png", dpi=300, bbox_inches="tight") # Saves the figure
    plt.close()

    # ---- IUCN polygon with GBIF points map ----
    fig, ax = plt.subplots(figsize=(10, 8))
    world.plot(ax=ax, color="grey", linewidth=1, edgecolor="black") # Plots the world map as background
    iucn.plot(ax=ax, color="green", alpha=0.5, edgecolor="darkgreen") # Plots the IUCN polygon in green
    coords_cleaned.plot(ax=ax, color="red", markersize=20) # Plots the GBIF points in red
    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_title(f"{species_name} - IUCN polygon with GBIF points")
    plt.savefig(f"{species_output_dir}/IUCN polygon with GBIF points.png", dpi=300, bbox_inches="tight") # Saves the figure
    plt.close()
    # Merges all IUCN polygons into one combined species range
    species_range = iucn.union_all()
    points_inside_range=coords_cleaned[coords_cleaned.intersects(species_range)].copy() # Keeps only the GBIF points that fall inside/intersect the IUCN range
    # Removes the geometry column before saving as CSV
    points_inside_range = points_inside_range.drop(columns="geometry")
    points_inside_range.to_csv(f"{species_output_dir}/validated_points.csv", index=False)

# Function to generate background points for SDM modeling
def background_points():
    presence_gdf=pd.read_csv(f"{species_output_dir}/validated_points.csv") # Loads the validated presence points from CSV
    presence_gdf=gpd.GeoDataFrame(presence_gdf, # Converts the presence points into a GeoDataFrame
        geometry=gpd.points_from_xy(presence_gdf["lon"], presence_gdf["lat"]), crs="EPSG:4326")
    iucn = gpd.read_file(f"{species_output_dir}/filtered_iucn.geojson")
    presence_proj = presence_gdf.to_crs("EPSG:3857") # Projects presence points to EPSG:3857 so distance buffering is in meters
    iucn_proj = iucn.to_crs("EPSG:3857") # Projects the IUCN polygon to the same CRS
    # Buffers distance around presence points
    # 10 * 1000 means 10 kilometers
    buffer_m = 10 * 1000
    # Creates one merged buffer area around all presence points
    presence_buffer_union = presence_proj.buffer(buffer_m).union_all()
    iucn_union = iucn_proj.union_all()
    # Removes the buffered presence area from the IUCN range
    # This helps avoid selecting background points too close to presence points
    bg_area_proj = iucn_union.difference(presence_buffer_union)
    # Converts the background sampling area back to the original IUCN CRS
    bg_area = gpd.GeoSeries([bg_area_proj], crs="EPSG:3857").to_crs(iucn.crs).iloc[0]

    minx, miny, maxx, maxy = bg_area.bounds # Gets the bounding box of the background sampling area

    rng = np.random.default_rng(42)
    points = []
    used_coords = set()
    # Keeps generating random points until we have the required number of background points
    while len(points) < num_of_bg_points:
        x = rng.uniform(minx, maxx) # Generates a random longitude within the background area's bounding box
        y = rng.uniform(miny, maxy) # Generates a random latitude within the background area's bounding box
        p = Point(x, y) # Creates a point from the random x and y values

        coord = (round(x, 6), round(y, 6))
        # Skips the point if it is outside the actual background area
        if not bg_area.contains(p):
            continue
        # Skips the point if the same coordinate was already selected
        if coord in used_coords:
            continue

        used_coords.add(coord)
        points.append(p)
    # Converts the background points into a GeoDataFrame
    bg = gpd.GeoDataFrame(geometry=points, crs=iucn.crs)
    bg["lon"] = bg.geometry.x
    bg["lat"] = bg.geometry.y
    bg = bg[["lon", "lat"]]
    # Saves background points to a CSV file
    bg.to_csv(f"{species_output_dir}/background_points.csv", index=False)

# Function to combine presence points and background points into one dataset
def merge_points():
    presence_df=pd.read_csv(f"{species_output_dir}/validated_points.csv")# Loads validated presence points from CSV
    background_df=pd.read_csv(f"{species_output_dir}/background_points.csv")# Loads generated background points from CSV
    # Adds a label column for background points
    # 0 means absence/background
    background_df['presence']=0
    # Adds a label column for presence points
    # 1 means species is present
    presence_df['presence']=1
    merged_df=pd.concat([presence_df,background_df], ignore_index=True) # Combines presence and background points into one DataFrame
    # Saves the merged dataset to a CSV file
    merged_df.to_csv(f"{species_output_dir}/merged_points.csv", index=False)

# Function to extract bioclimatic variable values for presence and background points
def extract_bioclim():
    iucn = gpd.read_file(f"{species_output_dir}/filtered_iucn.geojson")
    iucn_geom = iucn.union_all()
    bio_path=Path(config['climate_data']) # Reads the folder path where the climate raster files are stored
    bio_layers = {} # Dictionary to store all bioclimatic raster layers
    # Loads all 19 WorldClim bioclimatic raster files
    for i in range(1, 20):
        f = bio_path / f"wc2.1_2.5m_bio_{i}.tif"
        # Opens the raster file and remove the extra band dimension
        bio_layers[f"bio{i}"] = rxr.open_rasterio(f, masked=True).squeeze()

    # Crops each bioclimatic raster to the IUCN species range
    bio_crop = {
    name: layer.rio.clip([iucn_geom], iucn.crs, drop=True)
    for name, layer in bio_layers.items()}
    # Converts cropped raster values into a DataFrame
    # Each column represents one bioclimatic variable
    bio_df = pd.DataFrame({
    name: layer.values.ravel()
    for name, layer in bio_crop.items()})

    bio_df = bio_df.dropna() # Removes rows with missing raster values

    corr_matrix = bio_df.corr().abs() # Calculates absolute correlation between all bioclimatic variables
    # Creates a correlation heatmap
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Heatmap of Bioclimatic Variables")
    # Saves the correlation heatmap as a PNG file
    plt.savefig(f"{species_output_dir}/Correlation Heatmap of Bioclimatic Variables.png", dpi=300, bbox_inches="tight")
    plt.close()
    # Keeps only the selected bioclimatic variables from the config file
    bio_crop={name:layer for name,layer in bio_crop.items() if name in bio_vars}
    # Loads the merged presence/background points
    merged_df=pd.read_csv(f"{species_output_dir}/merged_points.csv")
    merged_df=gpd.GeoDataFrame(merged_df,
        geometry=gpd.points_from_xy(merged_df["lon"], merged_df["lat"]),crs="EPSG:4326")
    # Extracts raster values for each selected bioclimatic variable
    for name, raster in bio_crop.items():
        values = []
        # Loops through each point location
        for x, y in zip(merged_df.geometry.x, merged_df.geometry.y):
                # Gets the nearest raster value for the point location
                val = raster.sel(x=x, y=y, method="nearest").item()
                values.append(val)
        # Adds the extracted bioclimatic values as a new column
        merged_df[name] = values

    merged_df=merged_df.dropna() # Removes rows where any extracted bioclimatic value is missing
    merged_df = merged_df.drop(columns="geometry")
    # Saves the final dataset with bioclimatic variables
    merged_df.to_csv(f"{species_output_dir}/merged_points_with_bioclim.csv", index=False)

# Function to train the MaxEnt model, evaluate it, and create a suitability map
def model_training():
    # Loads the dataset that contains points + extracted bioclimatic values
    merged_df=pd.read_csv(f"{species_output_dir}/merged_points_with_bioclim.csv")

    X = merged_df[bio_vars] # Selects the predictor variables (chosen bioclimatic variables)
    # Selects the target column
    # presence = 1 means species present
    # presence = 0 means background point
    y = merged_df["presence"]

    # Splits the data into training and testing sets
    # 80% for training, 20% for testing
    # stratify=y keeps the same class balance in both sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )
    # Creates the MaxEnt model for training
    # transform="cloglog" gives suitability values between 0 and 1
    # beta_multiplier controls regularization (model smoothness)
    model = ela.MaxentModel(
    transform="cloglog",
    beta_multiplier=2.0
    )

    model.fit(X_train, y_train) # Trains the model using the training data

    train_score = model.predict(X_train) # Predicts suitability scores for the training data

    fpr, tpr, thresholds = roc_curve(y_train, train_score) # Calculates ROC curve values for training data
    roc_auc = auc(fpr, tpr) # Calculates AUC score for training data
    # Plots the training ROC curve
    plt.figure(figsize=(10, 8))
    plt.plot(fpr, tpr, label=f"AUC = {roc_auc:.3f}")
    plt.plot([0, 1], [0, 1], linestyle="--") # diagonal reference line

    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Train AUC-ROC Curve")
    plt.legend()
    plt.grid(True)
    # Saves the training ROC plot
    plt.savefig(f"{species_output_dir}/AUC-ROC.png", dpi=300, bbox_inches="tight")
    plt.close()

    test_score = model.predict(X_test) # Predicts suitability scores for the test data

    fpr, tpr, thresholds = roc_curve(y_test, test_score) # Calculates ROC curve values for test data
    test_auc = auc(fpr, tpr)
    # Plots the test ROC curve
    plt.figure(figsize=(10, 8))
    plt.plot(fpr, tpr, label=f"Test AUC = {test_auc:.3f}")
    plt.plot([0, 1], [0, 1], linestyle="--")

    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Test AUC-ROC Curve")
    plt.legend()
    plt.grid(True)
    # Saves the test ROC plot
    plt.savefig(f"{species_output_dir}/Test AUC-ROC.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Creates a new final model
    # This one is trained on the full dataset so it can be used for final prediction
    final_model = ela.MaxentModel(
    transform="cloglog",
    beta_multiplier=2.0
    )
    final_model.fit(X, y) # Trains the final model on all available data
    # Loads all 19 bioclimatic raster layers
    bio_path=Path(config['climate_data'])
    bio_layers = {}
    for i in range(1, 20):
        f = bio_path / f"wc2.1_2.5m_bio_{i}.tif"
        bio_layers[f"bio{i}"] = rxr.open_rasterio(f, masked=True).squeeze()

    # Defines the geographic extent for prediction
    # This extent covers the region where you want the suitability map
    xmin = 67
    xmax = 100
    ymin = 5
    ymax = 38
    # Crops only the selected bioclimatic layers to the chosen extent
    cropped_layers = {}
    for var in bio_vars:
        cropped_layers[var] = bio_layers[var].rio.clip_box(
            minx=xmin,
            miny=ymin,
            maxx=xmax,
            maxy=ymax
            ).squeeze()
    # Chooses the first selected raster as the template
    # Other rasters will be aligned to this one   
    template_name = bio_vars[0]
    template = cropped_layers[template_name]
    # Aligns all cropped rasters to the same grid as the template
    aligned_layers = {}
    for var in bio_vars:
        aligned_layers[var] = cropped_layers[var].rio.reproject_match(template).squeeze()
    # Stacks all aligned raster layers into one 3D array
    # shape will be: rows x cols x number_of_variables
    stack = np.stack([aligned_layers[var].values for var in bio_vars], axis=-1)

    # Finds pixels where all variables have valid values
    valid_mask = np.all(np.isfinite(stack), axis=-1)

    # Flattens only the valid pixels so they can be used for prediction
    X_pred = stack[valid_mask]
    X_pred_df = pd.DataFrame(X_pred, columns=bio_vars) # Converts prediction input into a DataFrame with correct column names
    # Predicts habitat suitability for all valid pixels
    pred = final_model.predict(X_pred_df)

    # Puts predicted values back into their correct raster positions
    suitability = np.full(valid_mask.shape, np.nan, dtype=float)
    suitability[valid_mask] = pred
    # Gets raster bounds for plotting
    bounds = template.rio.bounds()
    extent = [bounds[0], bounds[2], bounds[1], bounds[3]]
    # Plots the habitat suitability map
    x_coords = template.x.values
    y_coords = template.y.values

    plt.figure(figsize=(12, 8))

    img = plt.pcolormesh(
        x_coords,
        y_coords,
        suitability,
        shading="auto",
        cmap="RdYlGn_r",
        vmin=0,
        vmax=1)
    
    cbar = plt.colorbar(img)
    cbar.set_label("Habitat Suitability")
    cbar.set_ticks([0, 0.5, 1])
    cbar.set_ticklabels(["Low", "Medium", "High"])

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(f"Habitat Suitability Map - {species_name}")
    plt.savefig(f"{species_output_dir}/Suitability Map.png",dpi=300,bbox_inches="tight")
    plt.close()

    
# Defines the Airflow DAG for the SDM pipeline
with DAG(
    dag_id="sdm_pipeline", # Unique name for this DAG in the Airflow UI
    start_date=datetime(2026, 4, 19), # Date from which Airflow can start running this DAG
    schedule=None, # No automatic schedule; this DAG will run only when triggered manually
    catchup=False, # Do not run missed/backdated DAG runs from the past
    tags=["sdm"],
) as dag:

    # Airflow task to download species occurrence records from GBIF
    fetch_gbif_occurrences = PythonOperator(
    task_id="fetch_gbif_occurrences", # Task name shown in the Airflow UI
    python_callable=gbif_occurrences, # Python function that this task will run
    )

    # Airflow task to load and filter IUCN range data for the selected species
    load_and_filter_iucn_data = PythonOperator(
        task_id="load_and_filter_iucn_data", # Task name shown in the Airflow UI
        python_callable=load_iucn,
    )

    # Airflow task to validate GBIF points using the IUCN range polygon
    validate_points = PythonOperator(
        task_id="validate_points", # Task name shown in the Airflow UI
        python_callable=plot_iucn_and_gbif,
    )

    # Airflow task to create random background points inside the IUCN range
    create_background_points = PythonOperator(
        task_id="create_background_points", # Task name shown in the Airflow UI
        python_callable=background_points,
    )

    # Airflow task to merge presence points and background points into one dataset
    merge_presence_background_points = PythonOperator(
        task_id="merge_presence_background_points", # Task name shown in the Airflow UI
        python_callable=merge_points,
    )
    
    # Airflow task to extract bioclimatic values for presence and background points
    extract_bioclim_values_for_iucn_range = PythonOperator(
        task_id="extract_bioclim_values_for_iucn_range", # Task name shown in the Airflow UI
        python_callable=extract_bioclim,
    )

    # Airflow task to train the MaxEnt model and generate prediction outputs
    model_training_and_prediction = PythonOperator(
        task_id="model_training_and_prediction", # Task name shown in the Airflow UI
        python_callable=model_training,
    )

    # Airflow task to send an email after the SDM pipeline finishes successfully
    send_email = EmailOperator(
        task_id="send_email", # Task name shown in the Airflow UI
        # SMTP connection ID configured in Airflow
        # Airflow will use this connection to send the email
        conn_id="smtp_default",
        # Email address of the person who should receive the notification
        # This value is read from the config file
        to=config['notify_to'],
        subject=f"SDM analysis completed - {species_name}", # Subject line of the email
        # Email body content in HTML format
        html_content=f"""
        <h3>SDM analysis completed successfully</h3>
        <p><b>Species:</b> {species_name}</p>
        <p><b>Output folder:</b> {species_output_dir}</p>
        <p>The suitability map and related SDM outputs have been generated.</p> """,
    )


# Defines the order in which the Airflow tasks should run
# Each task starts only after the task before it completes successfully
fetch_gbif_occurrences >> load_and_filter_iucn_data >> validate_points >> create_background_points >> merge_presence_background_points >> extract_bioclim_values_for_iucn_range >> model_training_and_prediction >> send_email





