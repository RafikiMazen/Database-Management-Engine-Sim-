
public class SQLTerm {
	private String _strTableName;
	private String _strColumnName;
	private String _strOperator;
	private Object _objValue;
	
	public String get_strTableName() {
		return _strTableName;
	}

	public String get_strColumnName() {
		return _strColumnName;
	}

	public String get_strOperator() {
		return _strOperator;
	}

	public Object get_objValue() {
		return _objValue;
	}

	

	public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
		this._strTableName=_strTableName;
		this._strColumnName=_strColumnName;
		this._strOperator =_strOperator;
		this._objValue =_objValue;
	}

	

}
