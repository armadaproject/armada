export interface ColumnContext {
  columns: ColumnSpec[]
  selectedColumns: Set<string>
}

export interface ColumnSpec {
  id: string
  name: string
}

export default class JobTableColumnActions {
  private columnContext: ColumnContext
  private allColumns: Map<string, ColumnSpec>

  constructor(initialContext: ColumnContext) {
    this.columnContext = initialContext
    this.allColumns = new Map<string, ColumnSpec>()

    for (let col of initialContext.columns) {
      this.allColumns.set(col.id, col)
    }
  }

  addColumn(colSpec: ColumnSpec): ColumnContext {
    this.allColumns.set(colSpec.id, colSpec)

    return this.getContext()
  }

  deleteColumn(id: string): ColumnContext {
    if (this.allColumns.has(id)) {
      this.allColumns.delete(id)
    }

    if (this.columnContext.selectedColumns.has(id)) {
      this.columnContext.selectedColumns.delete(id)
    }

    return this.getContext()
  }

  editColumn(id: string, newName: string): ColumnContext {
    const newColSpec = {
      id: id,
      name: newName
    }
    this.allColumns.set(id, newColSpec)

    return this.getContext()
  }

  selectColumn(id: string, selected: boolean): ColumnContext {
    if (selected && this.allColumns.has(id)) {
      this.columnContext.selectedColumns.add(id)
    }

    if (!selected && this.columnContext.selectedColumns.has(id)) {
      this.columnContext.selectedColumns.delete(id)
    }

    return this.getContext()
  }

  getContext(): ColumnContext {
    return {
      columns: Array.from(this.allColumns.entries()).map((value) => value[1]),
      selectedColumns: new Set<string>(this.columnContext.selectedColumns),
    }
  }

  private addColumnsToMap(cols: ColumnSpec[]) {
    for (let col of cols) {
      this.allColumns.set(col.id, col)
    }
  }
}
