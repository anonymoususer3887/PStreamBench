package SmartGrid;

import java.io.Serializable;

public class AverageValue2 implements Serializable {
		public long timestamp;
		public int plug;
		public int household;
		public int house;
		public float localAvgLoad;
		
		

		public AverageValue2(long timestamp, int plug, int household, int house, float localAvgLoad) {
			super();
			this.timestamp = timestamp;
			this.plug = plug;
			this.household = household;
			this.house = house;
			this.localAvgLoad = localAvgLoad;
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

		public float getLocalAvgLoad() {
			return localAvgLoad;
		}

		public void setLocalAvgLoad(float localAvgLoad) {
			this.localAvgLoad = localAvgLoad;
		}

		@Override
		public String toString() {
			return "AverageValue2 [timestamp=" + timestamp + ", plug=" + plug + ", household=" + household + ", house="
					+ house + ", localAvgLoad=" + localAvgLoad + "]";
		}
		
		
		
		
		
		
}
