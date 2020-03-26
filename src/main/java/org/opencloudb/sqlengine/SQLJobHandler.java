package org.opencloudb.sqlengine;

import java.util.List;

public interface SQLJobHandler {

	void onHeader(String dataNode, byte[] header, List<byte[]> fields);

	boolean onRowData(String dataNode, byte[] rowData);

 	void finished(String dataNode, boolean failed);

}
