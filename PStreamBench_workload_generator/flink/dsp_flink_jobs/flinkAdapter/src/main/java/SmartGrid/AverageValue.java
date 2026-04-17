package SmartGrid;

import java.io.Serializable;

public class AverageValue implements Serializable {
		public long timestamp;
		public int house;
		public float globalAvgLoad;
		
		public AverageValue(long timestamp, int house, float globalAvgLoad) {
			super();
			this.timestamp = timestamp;
			this.house = house;
			this.globalAvgLoad = globalAvgLoad;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		public int getHouse() {
			return house;
		}

		public void setHouse(int house) {
			this.house = house;
		}

		public float getGlobalAvgLoad() {
			return globalAvgLoad;
		}

		public void setGlobalAvgLoad(float globalAvgLoad) {
			this.globalAvgLoad = globalAvgLoad;
		}

		@Override
		public String toString() {
			return "AverageValue [timestamp=" + timestamp + ", house=" + house + ", globalAvgLoad=" + globalAvgLoad
					+ "]";
		}
		
		
		
}
