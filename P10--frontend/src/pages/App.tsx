import React from 'react';
import '../styles/app.scss';
import {BrowserRouter as Router, Route, Routes} from "react-router-dom";
import MapExample from "./temp/MapExample";

function App() {
  return (
      <Router>
        <Routes>
          <Route path={'/'} element={<MapExample />}/>
        </Routes>
      </Router>
  );
}

export default App;
