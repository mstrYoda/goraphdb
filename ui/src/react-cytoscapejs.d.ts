declare module 'react-cytoscapejs' {
  import cytoscape from 'cytoscape'
  import { Component } from 'react'

  interface CytoscapeComponentProps {
    elements: cytoscape.ElementDefinition[]
    stylesheet?: cytoscape.Stylesheet[]
    layout?: cytoscape.LayoutOptions
    style?: React.CSSProperties
    cy?: (cy: cytoscape.Core) => void
    [key: string]: any
  }

  export default class CytoscapeComponent extends Component<CytoscapeComponentProps> {}
}
