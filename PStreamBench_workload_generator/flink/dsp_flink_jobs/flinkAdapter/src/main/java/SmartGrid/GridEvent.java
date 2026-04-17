package SmartGrid;
import java.io.Serializable;

public class GridEvent implements Serializable {
	
	int entryNumber; long timestamp; float value ; int property; int plug ; int household; int house;
	
	public GridEvent() {
		
	}

	public GridEvent(int entryNumber, long timestamp, float value, int property, int plug, int household,
			int house) {
		super();
		this.entryNumber = entryNumber;
		this.timestamp = timestamp;
		this.value = value;
		this.property = property;
		this.plug = plug;
		this.household = household;
		this.house = house;
	}

	public int getEntryNumber() {
		return entryNumber;
	}

	public void setEntryNumber(int entryNumber) {
		this.entryNumber = entryNumber;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	public int getProperty() {
		return property;
	}

	public void setProperty(int property) {
		this.property = property;
	}

	public int getPlug() {
		return plug;
	}

	public void setPlug(int plug) {
		this.plug = plug;
	}

	public int getHousehold() {
		return household;
	}

	public void setHousehold(int household) {
		this.household = household;
	}

	public int getHouse() {
		return house;
	}

	public void setHouse(int house) {
		this.house = house;
	}

	@Override
	public String toString() {
		return "GridEvent [entryNumber=" + entryNumber + ", timestamp=" + timestamp + ", value=" + value
				+ ", property=" + property + ", plug=" + plug + ", household=" + household + ", house=" + house + "]";
	}
	
	

}
