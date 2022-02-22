import React from 'react';
import '../styles/app.scss';
import {BrowserRouter as Router, Route, Routes} from "react-router-dom";
import MainLayout from "./temp/_layout";
import {SidebarProvider} from "../contexts/sidebarContext";

function App() {
    return (
        <Router>
            <SidebarProvider>
                <Routes>
                    <Route path={'/'} element={<MainLayout/>}/>
                </Routes>
            </SidebarProvider>
        </Router>
    );
}

export default App;
